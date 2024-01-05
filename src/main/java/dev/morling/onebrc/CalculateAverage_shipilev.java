/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class CalculateAverage_shipilev {

    private static final String FILE = "./measurements.txt";

    private static final int MMAP_CHUNK_SIZE = 16 * 1024 * 1024;
    private static final int BYTE_CHUNK_SIZE = 1 * 1024 * 1024;

    // ========================= Storage =========================

    private static final ArrayList<MeasurementsMap> ALL_MAPS = new ArrayList<>();

    private static final ThreadLocal<MeasurementsMap> MAPS = ThreadLocal.withInitial(new Supplier<MeasurementsMap>() {
        @Override
        public MeasurementsMap get() {
            MeasurementsMap m = new MeasurementsMap();
            synchronized (ALL_MAPS) {
                ALL_MAPS.add(m);
            }
            return m;
        }
    });

    // ========================= MEAT: PARSE AND COMPUTE =========================

    public static class MeasurementsMap {
        private int mapSize = 4096;
        private int mapSizeMask = mapSize - 1;

        private Bucket[] map;

        public MeasurementsMap() {
            map = new Bucket[mapSize];
        }

        private void rehash() {
            Bucket[] oldMap = map;
            mapSize *= 2;
            mapSizeMask = mapSize - 1;
            map = new Bucket[mapSize];
            merge(oldMap, false);
        }

        public static boolean arraysEquals(byte[] orig, ByteBuffer cand, int begin, int end) {
            int origLen = orig.length;
            int candLen = end - begin;
            if (origLen != candLen) {
                return false;
            }
            for (int i = 0; i < origLen; i++) {
                if (orig[i] != cand.get(begin + i)) {
                    return false;
                }
            }
            return true;
        }

        public Bucket bucket(ByteBuffer name, int nameBegin, int nameEnd, int hash) {
            int origIdx = hash & mapSizeMask;
            int idx = origIdx;

            while (true) {
                Bucket cur = map[idx];
                if (cur == null) {
                    // No bucket yet
                    byte[] copy = new byte[nameEnd - nameBegin];
                    name.get(nameBegin, copy, 0, nameEnd - nameBegin);
                    map[idx] = cur = new Bucket(copy, hash);
                    return cur;
                }
                else if (cur.rawNameHash == hash &&
                        arraysEquals(cur.rawName, name, nameBegin, nameEnd)) {
                    // Bucket hit!
                    return cur;
                }
                else {
                    // Keep searching until we hit rehash
                    idx = (idx + 1) & mapSizeMask;
                    if (idx == origIdx) {
                        rehash();
                        idx = hash & mapSizeMask;
                    }
                }
            }
        }

        public void merge(MeasurementsMap otherMap) {
            merge(otherMap.map, true);
        }

        private void merge(Bucket[] buckets, boolean allowRehash) {
            for (Bucket other : buckets) {
                if (other == null)
                    continue;
                int origIdx = other.rawNameHash & mapSizeMask;
                int idx = origIdx;
                while (true) {
                    Bucket cur = map[idx];
                    if (cur == null) {
                        // No bucket yet
                        map[idx] = other;
                        break;
                    }
                    else if (cur.rawNameHash == other.rawNameHash &&
                            Arrays.equals(cur.rawName, other.rawName)) {
                        // Bucket hit!
                        cur.merge(other);
                        break;
                    }
                    else {
                        // Keep searching
                        idx = (idx + 1) & mapSizeMask;
                        if (idx == origIdx) {
                            if (allowRehash) {
                                rehash();
                                idx = other.rawNameHash & mapSizeMask;
                            }
                            else {
                                throw new IllegalStateException("Cannot happen");
                            }
                        }
                    }
                }
            }
        }

        public Row[] rows() {
            Row[] rows = new Row[mapSize];
            int idx = 0;
            for (Bucket bucket : map) {
                if (bucket == null)
                    continue;
                rows[idx++] = new Row(
                        new String(bucket.rawName),
                        Math.round((double) bucket.min) / 10.0,
                        Math.round((double) bucket.sum / bucket.count) / 10.0,
                        Math.round((double) bucket.max) / 10.0);
            }

            return Arrays.copyOf(rows, idx);
        }

        private static class Bucket {
            private final byte[] rawName;
            private final int rawNameHash;

            private int min = Integer.MAX_VALUE;
            private int max = Integer.MIN_VALUE;
            private long sum;
            private long count;

            public Bucket(byte[] rawName, int rawNameHash) {
                this.rawName = rawName;
                this.rawNameHash = rawNameHash;
            }

            public void merge(int value) {
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                count++;
            }

            public void merge(Bucket s) {
                min = Math.min(min, s.min);
                max = Math.max(max, s.max);
                sum += s.sum;
                count += s.count;
            }
        }
    }

    public static class ProcessingTask extends RecursiveAction {
        private final MappedByteBuffer buf;
        private final int taskStart;
        private final int taskEnd;

        public ProcessingTask(MappedByteBuffer buf, int start, int end) {
            this.buf = buf;
            this.taskStart = start;
            this.taskEnd = end;
        }

        @Override
        protected void compute() {
            try {
                internalCompute();
            }
            catch (Exception e) {
                throw new IllegalStateException("Internal error", e); // YOLO
            }
        }

        private void internalCompute() throws Exception {
            if (taskEnd - taskStart > BYTE_CHUNK_SIZE) {
                // Split in half, figure out the boundary
                int mid = taskStart + (taskEnd - taskStart) / 2;

                // Do not split the line.
                while (buf.get(mid - 1) != '\n') {
                    mid--;
                }

                // Fork out
                ForkJoinTask.invokeAll(
                        new ProcessingTask(buf, taskStart, mid),
                        new ProcessingTask(buf, mid, taskEnd));
            }
            else if (taskStart < taskEnd) {
                MeasurementsMap map = MAPS.get();
                int length = taskEnd - taskStart;
                ByteBuffer slice = buf.slice(taskStart, length);
                seqCompute(map, slice, length);
            }
        }

        private void seqCompute(MeasurementsMap map, ByteBuffer slice, int length) throws IOException {
            int idx = 0;
            while (idx < length) {
                // Parse out the name, computing the hash on the fly.
                // Note the hash includes ';', but we do not really care.
                MeasurementsMap.Bucket bucket = null;
                {
                    int nameBegin = idx;
                    int nameHash = 0;
                    byte b = 0;
                    do {
                        b = slice.get(idx);
                        nameHash *= 31;
                        nameHash += b;
                        idx++;
                    } while (b != ';');
                    int nameEnd = idx - 1;

                    bucket = map.bucket(slice, nameBegin, nameEnd, nameHash);
                }

                // Parse out the temperature.
                {
                    int temp = 0;
                    int neg = 1;
                    if (slice.get(idx) == '-') {
                        neg = -1;
                        idx++;
                    }
                    int digit = 0;
                    do {
                        temp = temp * 10 + digit;
                        byte b = slice.get(idx);
                        digit = b - '0';
                        idx++;
                    } while (digit >= 0);

                    // Must be terminated at decimal point. Parse one more digit.
                    temp = temp * 10 + (slice.get(idx) - '0');
                    temp = temp * neg;
                    idx++;

                    bucket.merge(temp);
                }

                // Eat the rest of the digits and the EOL.
                while (slice.get(idx) != '\n') {
                    idx++;
                }
                idx++;
            }
        }
    }

    public static class MmapSplitTask extends RecursiveTask<ArrayList<ProcessingTask>> {
        private final FileChannel fc;
        private final long taskStart;
        private final long taskEnd;

        public MmapSplitTask(FileChannel fc, long taskStart, long taskEnd) {
            this.fc = fc;
            this.taskStart = taskStart;
            this.taskEnd = taskEnd;
        }

        protected ArrayList<ProcessingTask> compute() {
            try {
                return internalCompute();
            }
            catch (Exception e) {
                throw new IllegalStateException("Internal error", e); // YOLO
            }
        }

        protected ArrayList<ProcessingTask> internalCompute() throws IOException {
            ArrayList<ProcessingTask> tasks = new ArrayList<>();

            if (taskEnd - taskStart > MMAP_CHUNK_SIZE) {
                // Split in half, figure out the boundary
                long mid = taskStart + (taskEnd - taskStart) / 2;

                // Adjust mid to avoid splitting the line.
                {
                    int windowSize = (int) Math.min(64, mid);
                    int window = windowSize;
                    MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_ONLY, mid - windowSize, windowSize);
                    while (buf.get(window - 1) != '\n') {
                        window--;
                    }
                    mid = mid - windowSize + window;
                    UnsafeAccess.invokeCleaner(buf); // buf is unusable after this
                }

                MmapSplitTask f1 = new MmapSplitTask(fc, taskStart, mid);
                MmapSplitTask f2 = new MmapSplitTask(fc, mid, taskEnd);
                f1.fork();
                f2.fork();
                tasks.addAll(f2.join());
                tasks.addAll(f1.join());
            }
            else if (taskStart < taskEnd) {
                // We know size fits int at this point and that the line is not split.
                int size = (int) (taskEnd - taskStart);
                MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_ONLY, taskStart, size);
                tasks.add(new ProcessingTask(buf, 0, size));
            }
            return tasks;
        }
    }

    // ========================= Invocation =========================

    public static void main(String[] args) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(FILE, "r");
                FileChannel fc = file.getChannel()) {
            // Parallel prepare all mmapings, prepare the processing tasks.
            ArrayList<ProcessingTask> tasks = new MmapSplitTask(fc, 0, fc.size()).invoke();

            // Fork and join all processing tasks
            for (ProcessingTask task : tasks) {
                task.fork();
            }
            for (ProcessingTask task : tasks) {
                task.join();
                UnsafeAccess.invokeCleaner(task.buf);
            }

            // Merge all results from thread-local maps
            MeasurementsMap map = new MeasurementsMap();
            for (MeasurementsMap m : ALL_MAPS) {
                map.merge(m);
            }

            // ...and report them
            System.out.println(report(map));
        }
    }

    // ========================= Reporting =========================

    private static String report(MeasurementsMap map) {
        Row[] rows = map.rows();
        Arrays.sort(rows);

        StringBuilder sb = new StringBuilder(16384);
        sb.append("{");
        boolean first = true;
        for (Row r : rows) {
            if (first) {
                first = false;
            }
            else {
                sb.append(", ");
            }
            r.printTo(sb);
        }
        sb.append("}");
        return sb.toString();
    }

    private static class Row implements Comparable<Row> {
        private final String name;
        private final double min;
        private final double max;
        private final double avg;

        public Row(String name, double min, double avg, double max) {
            this.name = name;
            this.min = min;
            this.max = max;
            this.avg = avg;
        }

        @Override
        public int compareTo(Row o) {
            return name.compareTo(o.name);
        }

        public void printTo(StringBuilder sb) {
            sb.append(name);
            sb.append("=");
            sb.append(min);
            sb.append("/");
            sb.append(avg);
            sb.append("/");
            sb.append(max);
        }
    }

    // ========================= Utils =========================

    public static class UnsafeAccess {
        private static final Unsafe U;

        static {
            try {
                Field f = Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                U = (Unsafe) f.get(null);
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        public static void invokeCleaner(ByteBuffer bb) {
            U.invokeCleaner(bb);
        }
    }

}
