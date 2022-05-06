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
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** A class that writes cache data to memory segments. */
@Internal
public class MemorySegmentWriter<T> implements SegmentWriter<T> {
    private final LimitedSizeMemoryManager memoryManager;

    private final Path path;

    private final TypeSerializer<T> serializer;

    private final ManagedMemoryOutputStream outputStream;

    private final DataOutputView outputView;

    private int count;

    public MemorySegmentWriter(
            Path path,
            LimitedSizeMemoryManager memoryManager,
            TypeSerializer<T> serializer,
            long expectedSize)
            throws MemoryAllocationException {
        this.serializer = serializer;
        this.memoryManager = Preconditions.checkNotNull(memoryManager);
        this.path = path;
        this.outputStream = new ManagedMemoryOutputStream(memoryManager, expectedSize);
        this.outputView = new DataOutputViewStreamWrapper(outputStream);
        this.count = 0;
    }

    @Override
    public boolean addRecord(T record) {
        try {
            serializer.serialize(record, outputView);
            count++;
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public int getCount() {
        return this.count;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<Segment> finish() throws IOException {
        if (count > 0) {
            return Optional.of(
                    new Segment(
                            path,
                            count,
                            outputStream.getKey(),
                            outputStream.getSegments(),
                            (TypeSerializer<Object>) serializer));
        } else {
            memoryManager.releaseAll(outputStream.getKey());
            return Optional.empty();
        }
    }

    private static class ManagedMemoryOutputStream extends OutputStream {
        private final LimitedSizeMemoryManager memoryManager;

        private final int pageSize;

        private final Object key = new Object();

        private final List<MemorySegment> segments = new ArrayList<>();

        private int segmentIndex;

        private int segmentOffset;

        private int globalOffset;

        public ManagedMemoryOutputStream(LimitedSizeMemoryManager memoryManager, long expectedSize)
                throws MemoryAllocationException {
            this.memoryManager = memoryManager;
            this.pageSize = memoryManager.getPageSize();
            this.segmentIndex = 0;
            this.segmentOffset = 0;

            Preconditions.checkArgument(expectedSize >= 0);
            if (expectedSize > 0) {
                int numPages = (int) ((expectedSize + pageSize - 1) / pageSize);
                segments.addAll(memoryManager.allocatePages(getKey(), numPages));
            }
        }

        public Object getKey() {
            return key;
        }

        public List<MemorySegment> getSegments() {
            return segments;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            try {
                ensureCapacity(globalOffset + len);
            } catch (MemoryAllocationException e) {
                throw new IOException(e);
            }
            writeRecursive(b, off, len);
        }

        private void ensureCapacity(int capacity) throws MemoryAllocationException {
            Preconditions.checkArgument(capacity > 0);
            int requiredSegmentNum = (capacity - 1) / pageSize + 2 - segments.size();
            if (requiredSegmentNum > 0) {
                segments.addAll(memoryManager.allocatePages(getKey(), requiredSegmentNum));
            }
        }

        private void writeRecursive(byte[] b, int off, int len) {
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
