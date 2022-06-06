/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.iteration.datacache.nonkeyed;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** A class that writes cache data to memory segments. */
@Internal
class MemorySegmentWriter<T> implements SegmentWriter<T> {

    private final TypeSerializer<T> serializer;

    private final Path path;

    private final MemorySegmentPool segmentPool;

    private final ManagedMemoryOutputStream outputStream;

    private final DataOutputView outputView;

    private int count;

    MemorySegmentWriter(
            TypeSerializer<T> serializer,
            Path path,
            MemorySegmentPool segmentPool,
            long expectedSize)
            throws IOException {
        this.serializer = serializer;
        this.path = path;
        this.segmentPool = segmentPool;
        this.outputStream = new ManagedMemoryOutputStream(segmentPool, expectedSize);
        this.outputView = new DataOutputViewStreamWrapper(outputStream);
        this.count = 0;
    }

    @Override
    public boolean addRecord(T record) throws IOException {
        if (outputStream.getPos() >= DataCacheWriter.MAX_SEGMENT_SIZE) {
            return false;
        }
        try {
            serializer.serialize(record, outputView);
            count++;
            return true;
        } catch (IOException e) {
            if (e.getCause() instanceof MemoryAllocationException) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public int getCount() {
        return this.count;
    }

    @Override
    public Optional<Segment> finish() throws IOException {
        if (count > 0) {
            return Optional.of(new Segment(path, count, outputStream.getSegments()));
        } else {
            segmentPool.returnAll(outputStream.getSegments());
            return Optional.empty();
        }
    }

    private static class ManagedMemoryOutputStream extends OutputStream {
        private final MemorySegmentPool segmentPool;

        private final int pageSize;

        private final List<MemorySegment> segments = new ArrayList<>();

        private int segmentIndex;

        private int segmentOffset;

        private long globalOffset;

        public ManagedMemoryOutputStream(MemorySegmentPool segmentPool, long expectedSize)
                throws IOException {
            this.segmentPool = segmentPool;
            this.pageSize = segmentPool.pageSize();
            this.segmentIndex = 0;
            this.segmentOffset = 0;

            Preconditions.checkArgument(expectedSize >= 0);
            ensureCapacity(Math.max(expectedSize, 1L));
        }

        public long getPos() {
            return globalOffset;
        }

        public List<MemorySegment> getSegments() {
            return segments;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public void write(@Nullable byte[] b, int off, int len) throws IOException {
            ensureCapacity(globalOffset + len);
            writeRecursive(b, off, len);
        }

        private void ensureCapacity(long capacity) throws IOException {
            Preconditions.checkArgument(capacity > 0);
            int required =
                    (int) (capacity % pageSize == 0 ? capacity / pageSize : capacity / pageSize + 1)
                            - segments.size();

            List<MemorySegment> allocatedSegments = new ArrayList<>();
            for (int i = 0; i < required; i++) {
                MemorySegment memorySegment = segmentPool.nextSegment();
                if (memorySegment == null) {
                    segmentPool.returnAll(allocatedSegments);
                    throw new IOException(new MemoryAllocationException());
                }
                allocatedSegments.add(memorySegment);
            }

            segments.addAll(allocatedSegments);
        }

        private void writeRecursive(byte[] b, int off, int len) {
            if (len == 0) {
                return;
            }
            int currentLen = Math.min(len, pageSize - segmentOffset);
            segments.get(segmentIndex).put(segmentOffset, b, off, currentLen);
            segmentOffset += currentLen;
            globalOffset += currentLen;
            if (segmentOffset >= pageSize) {
                segmentIndex++;
                segmentOffset = 0;
                writeRecursive(b, off + currentLen, len - currentLen);
            }
        }
    }
}
