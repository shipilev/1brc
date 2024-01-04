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
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.RecursiveTask;

public class CalculateAverage_shipilev {

    private static final String FILE = "./measurements.txt";

    private static final int MMAP_CHUNK_SIZE = 128 * 1024 * 1024;
    private static final int BYTE_CHUNK_SIZE = 1 * 1024 * 1024;

    // Quick and dirty open-address hashmap
    private static class MeasurementsMap {
        private static final int MAP_SIZE = 2048;
        private static final int MAP_SIZE_MASK = MAP_SIZE - 1;

        private final Bucket[] map;

        public MeasurementsMap() {
            map = new Bucket[MAP_SIZE];
        }

        private int hashCode(byte[] bs, int begin, int end) {
            int hc = 1;
            for (int i = begin; i < end; i++) {
                hc = hc * 31 + bs[i];
            }
            return hc;
        }

        public void add(byte[] name, int nameBegin, int nameEnd, int temp) {
            int hash = hashCode(name, nameBegin, nameEnd);
            int origIdx = hash & MAP_SIZE_MASK;
            int idx = origIdx;

            while (true) {
                Bucket cur = map[idx];
                if (cur == null) {
                    // No bucket yet
                    map[idx] = cur = new Bucket(Arrays.copyOfRange(name, nameBegin, nameEnd), hash);
                    cur.merge(temp);
                    return;
                }
                else if (cur.rawNameHash == hash &&
                        Arrays.equals(cur.rawName, 0, cur.rawName.length, name, nameBegin, nameEnd)) {
                    // Hit!
                    cur.merge(temp);
                    return;
                }
                else {
                    // Keep searching
                    idx = (idx + 1) & MAP_SIZE_MASK;
                    if (idx == origIdx) {
                        // TODO: Rehash.
                        throw new IllegalStateException("Not implemented");
                    }
                }
            }
        }

        public void merge(MeasurementsMap otherMap) {
            for (Bucket other : otherMap.map) {
                if (other == null)
                    continue;
                int origIdx = other.rawNameHash & MAP_SIZE_MASK;
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
                        // Hit!
                        cur.merge(other);
                        break;
                    }
                    else {
                        // Keep searching
                        idx = (idx + 1) & MAP_SIZE_MASK;
                        if (idx == origIdx) {
                            // TODO: Rehash.
                            throw new IllegalStateException("Not implemented");
                        }
                    }
                }
            }
        }

        private static double convert(double value) {
            return Math.round(value * 10.0) / 100.0;
        }

        public void print(PrintStream ps) {
            Arrays.sort(map, (o1, o2) -> {
                // Squeeze nulls on one side.
                if (o1 == null && o2 == null)
                    return 0;
                if (o1 == null)
                    return -1;
                if (o2 == null)
                    return 1;
                return o1.stringName().compareTo(o2.stringName());
            });

            ps.print("{");
            boolean first = true;
            for (Bucket stats : map) {
                if (stats == null)
                    continue;

                if (first) {
                    first = false;
                }
                else {
                    ps.print(", ");
                }
                ps.print(stats.stringName());
                ps.print("=");
                ps.print(convert(stats.min));
                ps.print("/");
                ps.print(convert(stats.sum / stats.count));
                ps.print("/");
                ps.print(convert(stats.max));
            }
            ps.print("}");
        }

        private static class Bucket {
            private final byte[] rawName;
            private final int rawNameHash;
            private String stringName;

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

            public String stringName() {
                if (stringName == null) {
                    stringName = new String(rawName);
                }
                return stringName;
            }
        }
    }

    public static class ProcessingTask extends WrapperTask {
        private static final ThreadLocal<byte[]> BUFFERS = ThreadLocal.withInitial(() -> new byte[BYTE_CHUNK_SIZE]);

        private final MappedByteBuffer buf;
        private final int taskStart;
        private final int taskSize;

        public ProcessingTask(MappedByteBuffer buf, int start, int size) {
            this.buf = buf;
            this.taskStart = start;
            this.taskSize = size;
        }

        @Override
        protected MeasurementsMap internalCompute() throws Exception {
            if (taskSize > BYTE_CHUNK_SIZE) {
                return split();
            }
            else {
                return seqCompute();
            }
        }

        private int parseCentigrades(byte[] src, int start, int end) {
            int temp = 0;
            int n = 1;
            if (src[start] == '-') {
                n = -1;
                start++;
            }
            for (int c = start; c <= end; c++) {
                byte b = src[c];
                if (b == '.') {
                    // Parse one more digit and exit.
                    end = c + 1;
                    continue;
                }
                int digit = b - '0';
                temp = temp * 10 + digit;
            }
            return temp * n;
        }

        private MeasurementsMap seqCompute() throws IOException {
            MeasurementsMap map = new MeasurementsMap();

            // Read the entire buffer into array: avoid range checks on buffer access.
            byte[] byteBuf = BUFFERS.get();
            buf.get(taskStart, byteBuf, 0, taskSize);

            int idx = 0;
            while (idx < taskSize) {
                int nameBegin = idx;
                idx += 3; // We know the name is at least 3 characters
                while (byteBuf[idx] != ';') {
                    idx++;
                }
                int nameEnd = idx;

                int tempBegin = idx + 1;
                idx += 3; // We know the temp is at least 3 characters
                while (byteBuf[idx] != '\n') {
                    idx++;
                }
                int tempEnd = idx;

                // Eat the EOL
                idx++;

                int temp = parseCentigrades(byteBuf, tempBegin, tempEnd);

                map.add(byteBuf, nameBegin, nameEnd, temp);
            }

            return map;
        }

        private MeasurementsMap split() throws IOException {
            int start = taskStart;
            int hardEnd = taskStart + taskSize;

            while (true) {
                int end = Math.min(start + BYTE_CHUNK_SIZE, hardEnd);
                if (start >= end) {
                    break;
                }

                // Do not split the line.
                while (buf.get(end - 1) != '\n') {
                    end--;
                }

                // Fork out
                int size = (end - start);
                forkOut(new ProcessingTask(buf, start, size));
                start += size;
            }

            return joinAll();
        }
    }

    public static class MmapSplitTask extends WrapperTask {
        private final FileChannel fc;

        public MmapSplitTask(FileChannel fc) {
            this.fc = fc;
        }

        @Override
        protected MeasurementsMap internalCompute() throws IOException {
            long start = 0;
            long hardEnd = fc.size();

            while (true) {
                long end = Math.min(start + MMAP_CHUNK_SIZE, hardEnd);
                if (start >= end) {
                    break;
                }

                // We know size fits int at this point.
                int size = (int) (end - start);

                // Do not split the line.
                MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_ONLY, start, size);
                while (buf.get(size - 1) != '\n') {
                    size--;
                }

                forkOut(new ProcessingTask(buf, 0, size));
                start += size;
            }

            return joinAll();
        }
    }

    public static abstract class WrapperTask extends RecursiveTask<MeasurementsMap> {
        private final ArrayList<WrapperTask> wts = new ArrayList<>();

        @Override
        protected MeasurementsMap compute() {
            try {
                return internalCompute();
            }
            catch (Exception e) {
                throw new IllegalStateException("Internal error", e); // YOLO
            }
        }

        protected void forkOut(WrapperTask t) {
            t.fork();
            wts.add(t);
        }

        protected MeasurementsMap joinAll() {
            MeasurementsMap map = new MeasurementsMap();
            for (WrapperTask wt : wts) {
                map.merge(wt.join());
            }
            return map;
        }

        protected abstract MeasurementsMap internalCompute() throws Exception;
    }

    public static class UnsafeArray {
        private static final Unsafe U;
        public static final long SCALE;
        public static final long BASE;

        static {
            try {
                Field f = Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                U = (Unsafe) f.get(null);
                BASE = U.arrayBaseOffset(byte[].class);
                SCALE = U.arrayIndexScale(byte[].class);

            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        public static byte get(byte[] b, int idx) {
            return U.getByte(b, BASE + idx * SCALE);
        }

        public static int getPrefix(byte[] b) {
            return U.getByte(b, BASE);
        }

    }

    public static void main(String[] args) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(FILE, "r");
                FileChannel fc = file.getChannel()) {
            MeasurementsMap map = new MmapSplitTask(fc).invoke();
            map.print(System.out);
        }
    }

}
