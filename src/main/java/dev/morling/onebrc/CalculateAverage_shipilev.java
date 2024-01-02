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

import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RecursiveTask;

public class CalculateAverage_shipilev {

    private static final String FILE = "./measurements.txt";

    private static final int MAP_SIZE = 2048;
    private static final int MAP_SIZE_MASK = MAP_SIZE - 1;

    private static final int CHUNK_SIZE = 1 * 1024 * 1024;

    // Quick and dirty open-address hashmap
    private static class MeasurementsMap {
        private final Bucket[] map;

        public MeasurementsMap() {
            map = new Bucket[MAP_SIZE];
        }

        public void add(byte[] name, int temp) {
            int hash = Arrays.hashCode(name);
            int idx = hash & MAP_SIZE_MASK;

            while (true) {
                Bucket curStat = map[idx];
                if (curStat == null) {
                    // No bucket yet
                    map[idx] = curStat = new Bucket(name, hash);
                    curStat.merge(temp);
                    return;
                }
                else if (curStat.rawNameHash == hash &&
                        Arrays.equals(curStat.rawName, name)) {
                    // Hit!
                    curStat.merge(temp);
                    return;
                }
                else {
                    // Keep searching
                    idx = (idx + 1) & MAP_SIZE_MASK;
                }
            }
        }

        public void merge(MeasurementsMap other) {
            for (Bucket otherStats : other.map) {
                if (otherStats == null)
                    continue;
                int idx = otherStats.rawNameHash & MAP_SIZE_MASK;
                while (true) {
                    Bucket curStat = map[idx];
                    if (curStat == null) {
                        // No bucket yet
                        map[idx] = otherStats;
                        break;
                    }
                    else if (curStat.rawNameHash == otherStats.rawNameHash &&
                            Arrays.equals(curStat.rawName, otherStats.rawName)) {
                        // Hit!
                        curStat.merge(otherStats);
                        break;
                    }
                    else {
                        // Keep searching
                        idx = (idx + 1) & MAP_SIZE_MASK;
                    }
                }
            }
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 100.0;
        }

        public void print(PrintStream ps) {
            Arrays.sort(map, (o1, o2) -> {
                if (o1 == null)
                    return -1; // YOLO
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
                ps.print(round(stats.min));
                ps.print("/");
                ps.print(round(stats.sum / stats.count));
                ps.print("/");
                ps.print(round(stats.max));
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

    public static class ProcessingTask extends RecursiveTask<MeasurementsMap> {
        private final FileChannel fc;
        private final long taskStart;
        private final long taskSize;

        public ProcessingTask(FileChannel fc, long taskStart, long taskSize) {
            this.fc = fc;
            this.taskStart = taskStart;
            this.taskSize = taskSize;
        }

        @Override
        protected MeasurementsMap compute() {
            try {
                if (taskSize <= CHUNK_SIZE) {
                    return internalCompute();
                }
                else {
                    return split();
                }
            }
            catch (Exception e) {
                throw new IllegalStateException("Internal error", e); // YOLO
            }
        }

        // TODO: This could actually be generic, if we track where the decimal point is.
        private int parseCentigrade(byte[] src, int start, int end) {
            int temp = 0;
            boolean negative = false;
            for (int c = start; c < end; c++) {
                byte b = src[c];
                if (b == '-') {
                    negative = true;
                    continue;
                }
                if (b == '.') {
                    continue;
                }
                int digit = b - '0';
                temp = temp * 10 + digit;
            }
            if (negative) {
                return -temp;
            }
            else {
                return temp;
            }
        }

        public static class ByteArraysPool {
            private static final ConcurrentLinkedQueue<byte[]> QS = new ConcurrentLinkedQueue<>();

            public static byte[] acquire() {
                byte[] bs = QS.poll();
                if (bs != null) {
                    return bs;
                }

                return new byte[CHUNK_SIZE];
            }

            public static void release(byte[] bs) {
                QS.add(bs);
            }
        }

        private MeasurementsMap internalCompute() throws IOException {
            MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_ONLY, taskStart, taskSize);

            int size = (int) taskSize;

            MeasurementsMap map = new MeasurementsMap();

            // Read the entire buffer into array: avoid range checks on buffer access.
            // TODO: Can do Unsafe here to avoid bounds-checks on byte[] as well.
            byte[] byteBuf = ByteArraysPool.acquire();
            buf.get(byteBuf, 0, size);

            int idx = 0;
            while (idx < size) {
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

                byte[] name = Arrays.copyOfRange(byteBuf, nameBegin, nameEnd);
                int temp = parseCentigrade(byteBuf, tempBegin, tempEnd);

                map.add(name, temp);
            }

            ByteArraysPool.release(byteBuf);

            return map;
        }

        private MeasurementsMap split() throws IOException {
            Collection<ProcessingTask> pts = new ArrayList<>();

            long start = taskStart;
            long hardEnd = taskStart + taskSize;

            while (true) {
                long end = Math.min(start + CHUNK_SIZE, hardEnd);
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

                // Fork out
                ProcessingTask pt = new ProcessingTask(fc, start, size);
                pt.fork();
                pts.add(pt);
                start += size;
            }

            // Aggregate the subtasks
            MeasurementsMap map = new MeasurementsMap();
            for (ProcessingTask pt : pts) {
                map.merge(pt.join());
            }
            return map;
        }
    }

    public static void main(String[] args) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(FILE, "r");
                FileChannel fc = file.getChannel()) {
            MeasurementsMap map = new ProcessingTask(fc, 0, fc.size()).invoke();
            map.print(System.out);
        }
    }

}
