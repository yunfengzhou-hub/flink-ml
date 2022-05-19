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
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryReservationException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** Reads the cached data from a list of segments. */
public class DataCacheReader<T> implements Iterator<T> {

    private final MemoryManager memoryManager;

    private final TypeSerializer<T> serializer;

    private final List<Segment> segments;

    private SegmentReader<T> currentReader;

    private MemorySegmentWriter<T> cacheWriter;

    private int segmentIndex;

    public DataCacheReader(
            TypeSerializer<T> serializer, MemoryManager memoryManager, List<Segment> segments) {
        this(serializer, memoryManager, segments, new Tuple2<>(0, 0));
    }

    public DataCacheReader(
            TypeSerializer<T> serializer,
            MemoryManager memoryManager,
            List<Segment> segments,
            Tuple2<Integer, Integer> readerPosition) {
        this.memoryManager = memoryManager;
        this.serializer = serializer;
        this.segments = segments;
        this.segmentIndex = readerPosition.f0;

        createSegmentReaderAndCache(readerPosition.f0, readerPosition.f1);
    }

    private void createSegmentReaderAndCache(int index, int startOffset) {
        try {
            cacheWriter = null;

            if (index >= segments.size()) {
                currentReader = null;
                return;
            }

            currentReader = SegmentReader.create(serializer, segments.get(index), startOffset);

            boolean shouldCacheInMemory =
                    startOffset == 0
                            && currentReader instanceof FsSegmentReader
                            && MemoryUtils.isMemoryEnoughForCache(memoryManager);

            if (shouldCacheInMemory) {
                cacheWriter =
                        new MemorySegmentWriter<>(
                                segments.get(index).getPath(),
                                memoryManager,
                                serializer,
                                segments.get(index).getFsSize());
            }
        } catch (MemoryReservationException e) {
            cacheWriter = null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return currentReader != null && currentReader.hasNext();
    }

    @Override
    public T next() {
        try {
            T record = currentReader.next();

            if (cacheWriter != null) {
                if (!cacheWriter.addRecord(record)) {
                    cacheWriter
                            .finish()
                            .ifPresent(x -> memoryManager.releaseAllMemory(x.getPath()));
                    cacheWriter = null;
                }
            }

            if (!currentReader.hasNext()) {
                currentReader.close();
                if (cacheWriter != null) {
                    cacheWriter
                            .finish()
                            .ifPresent(
                                    x ->
                                            segments.get(segmentIndex)
                                                    .setCacheInfo(
                                                            x.getCache(), x.getTypeSerializer()));
                }
                segmentIndex++;
                createSegmentReaderAndCache(segmentIndex, 0);
            }

            return record;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Tuple2<Integer, Integer> getPosition() {
        if (currentReader == null) {
            return new Tuple2<>(segments.size(), 0);
        }

        return new Tuple2<>(segmentIndex, currentReader.getOffset());
    }
}
