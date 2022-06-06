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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/** A class that reads data cached in memory. */
@Internal
class MemorySegmentReader<T> implements SegmentReader<T> {

    private final TypeSerializer<T> serializer;

    private final DataInputView inputView;

    private final int totalCount;

    private int count;

    MemorySegmentReader(TypeSerializer<T> serializer, Segment segment, int startOffset)
            throws IOException {
        ManagedMemoryInputStream inputStream = new ManagedMemoryInputStream(segment.getCache());
        this.inputView = new DataInputViewStreamWrapper(inputStream);
        this.serializer = serializer;
        this.totalCount = segment.getCount();
        this.count = 0;

        for (int ignored = 0; ignored < startOffset; ignored++) {
            next();
        }
    }

    @Override
    public boolean hasNext() {
        return count < totalCount;
    }

    @Override
    public T next() throws IOException {
        T ret = serializer.deserialize(inputView);
        count++;
        return ret;
    }

    @Override
    public void close() {}

    private static class ManagedMemoryInputStream extends InputStream {
        private final List<MemorySegment> segments;

        private int segmentIndex;

        private int segmentOffset;

        public ManagedMemoryInputStream(List<MemorySegment> segments) {
            this.segments = segments;
            this.segmentIndex = 0;
            this.segmentOffset = 0;
        }

        @Override
        public int read() throws IOException {
            int ret = segments.get(segmentIndex).get(segmentOffset) & 0xff;
            segmentOffset += 1;
            if (segmentOffset >= segments.get(segmentIndex).size()) {
                segmentIndex++;
                segmentOffset = 0;
            }
            return ret;
        }

        @Override
        public int read(@Nullable byte[] b, int off, int len) throws IOException {
            if (segments.size() == segmentIndex) {
                return 0;
            }

            int currentLen = Math.min(segments.get(segmentIndex).size() - segmentOffset, len);
            segments.get(segmentIndex).get(segmentOffset, b, off, currentLen);
            segmentOffset += currentLen;
            if (segmentOffset >= segments.get(segmentIndex).size()) {
                segmentIndex++;
                segmentOffset = 0;
                return currentLen + read(b, off + currentLen, len - currentLen);
            }
            return currentLen;
        }
    }
}
