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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** Reads the cached data from a list of segments. */
@Internal
public class DataCacheIterator<T> implements Iterator<T> {

    private final TypeSerializer<T> serializer;

    private final List<Segment> segments;

    private SegmentReader<T> currentReader;

    private int segmentIndex;

    private int pos;

    DataCacheIterator(TypeSerializer<T> serializer, List<Segment> segments) {
        this.segments = segments;
        this.serializer = serializer;
        createSegmentReader(0, 0);
    }

    /** Changes the read position to the given value. */
    public void setPos(int pos) throws Exception {
        Preconditions.checkArgument(pos >= 0);
        Tuple2<Integer, Integer> readerPosition = getReaderPosition(pos);
        createSegmentReader(readerPosition.f0, readerPosition.f1);
        this.segmentIndex = readerPosition.f0;
        this.pos = pos;
    }

    /** Gets the number of records that have been read so far. */
    public int getPos() {
        return pos;
    }

    @Override
    public boolean hasNext() {
        return currentReader != null && currentReader.hasNext();
    }

    @Override
    public T next() {
        try {
            T record = currentReader.next();

            if (!currentReader.hasNext()) {
                currentReader.close();
                segmentIndex++;
                createSegmentReader(segmentIndex, 0);
            }

            pos++;

            return record;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Tuple2<Integer, Integer> getReaderPosition(int pos) throws Exception {
        int segmentIndex = 0;
        do {
            int segmentCount = getSegmentCount(segments.get(segmentIndex));
            if (segmentCount <= pos) {
                pos -= segmentCount;
                segmentIndex++;
            } else {
                return Tuple2.of(segmentIndex, pos);
            }
        } while (segmentIndex < segments.size());

        throw new Exception(
                "Failed to get reader position from pos "
                        + pos
                        + ": value larger than total number of records in data cache.");
    }

    private static int getSegmentCount(Segment segment) {
        if (segment.getMemorySegment() != null) {
            return segment.getMemorySegment().getCount();
        } else {
            return segment.getFileSegment().getCount();
        }
    }

    private void createSegmentReader(int index, int startOffset) {
        try {
            if (index >= segments.size()) {
                currentReader = null;
                return;
            }

            Segment segment = segments.get(segmentIndex);
            if (segment.getMemorySegment() != null) {
                currentReader = new MemorySegmentReader<>(serializer, segment, startOffset);
            } else {
                currentReader = new FileSegmentReader<>(serializer, segment, startOffset);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
