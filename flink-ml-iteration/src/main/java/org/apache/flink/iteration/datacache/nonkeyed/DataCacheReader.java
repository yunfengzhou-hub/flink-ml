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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** Reads the cached data from a list of segments. */
public class DataCacheReader<T> implements Iterator<T> {

    /** The tool to deserialize bytes into records. */
    private final TypeSerializer<T> serializer;

    /** The segments where to read the records from. */
    private final List<Segment> segments;

    /** The current reader for next records. */
    private SegmentReader<T> currentReader;

    /** The index of the segment that current reader reads from. */
    private int segmentIndex;

    /** The number of records that have been read through current reader so far. */
    private int segmentCount;

    public DataCacheReader(TypeSerializer<T> serializer, List<Segment> segments) {
        this(serializer, segments, Tuple2.of(0, 0));
    }

    public DataCacheReader(
            TypeSerializer<T> serializer,
            List<Segment> segments,
            Tuple2<Integer, Integer> readerPosition) {
        this.serializer = serializer;
        this.segments = segments;
        this.segmentIndex = readerPosition.f0;
        this.segmentCount = readerPosition.f1;

        createSegmentReader(readerPosition.f0, readerPosition.f1);
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
                segmentCount = 0;
                createSegmentReader(segmentIndex, segmentCount);
            }

            segmentCount++;

            return record;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Tuple2<Integer, Integer> getPosition() {
        return new Tuple2<>(segmentIndex, segmentCount);
    }

    private void createSegmentReader(int index, int startOffset) {
        try {
            if (index >= segments.size()) {
                currentReader = null;
                return;
            }

            Segment segment = segments.get(segmentIndex);
            if (segment.isCached()) {
                currentReader = new MemorySegmentReader<>(serializer, segment, startOffset);
            } else {
                currentReader = new FileSegmentReader<>(serializer, segment, startOffset);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
