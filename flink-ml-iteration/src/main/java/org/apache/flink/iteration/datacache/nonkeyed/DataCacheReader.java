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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.memory.MemoryManager;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** Reads the cached data from a list of paths. */
public class DataCacheReader<T> implements Iterator<T> {

    @Nullable private final MemoryManager memoryManager;

    private final TypeSerializer<T> serializer;

    private final List<Segment> segments;

    @Nullable private SegmentReader currentSegmentReader;

    private MemorySegmentWriter cacheWriter;

    private int segmentIndex;

    public DataCacheReader(
            MemoryManager memoryManager, TypeSerializer<T> serializer, List<Segment> segments) {
        this(memoryManager, serializer, segments, new Tuple2<>(0, 0));
    }

    public DataCacheReader(
            MemoryManager memoryManager,
            TypeSerializer<T> serializer,
            List<Segment> segments,
            Tuple2<Integer, Integer> readerPosition) {
        this.memoryManager = memoryManager;
        this.serializer = serializer;
        this.segments = segments;
        this.segmentIndex = readerPosition.f0;

        createSegmentReaderAndCache(readerPosition.f0, readerPosition.f1);
    }

    private void createSegmentReaderAndCache(int index, int startOffset) {
        if (index >= segments.size()) {
            currentSegmentReader = null;
            cacheWriter = null;
            return;
        }

        try {
            currentSegmentReader = SegmentReader.create(segments.get(index), startOffset);

            boolean shouldCacheInMemory =
                    startOffset == 0
                            && currentSegmentReader instanceof FsSegmentReader
                            && memoryManager != null
                            && ((double) memoryManager.availableMemory())
                                            / memoryManager.getMemorySize()
                                    > 0.5;

            if (shouldCacheInMemory) {
                cacheWriter = new MemorySegmentWriter(segments.get(index).getPath(), memoryManager);
            } else {
                cacheWriter = null;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return currentSegmentReader != null && currentSegmentReader.hasNext();
    }

    @Override
    public T next() {
        try {
            byte[] bytes = currentSegmentReader.next();

            if (cacheWriter != null) {
                if (!cacheWriter.addRecord(bytes)) {
                    cacheWriter
                            .finish()
                            .ifPresent(
                                    x ->
                                            x.getBufferedMemorySegment()
                                                    .forEach(memoryManager::release));
                    cacheWriter = null;
                }
            }

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputView inputView = new DataInputViewStreamWrapper(byteArrayInputStream);
            T next = serializer.deserialize(inputView);

            if (!currentSegmentReader.hasNext()) {
                currentSegmentReader.close();
                if (cacheWriter != null) {
                    cacheWriter
                            .finish()
                            .ifPresent(
                                    x ->
                                            segments.get(segmentIndex)
                                                    .setBufferedMemorySegments(
                                                            x.getBufferedMemorySegment()));
                }
                segmentIndex++;
                createSegmentReaderAndCache(segmentIndex, 0);
            }

            return next;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Tuple2<Integer, Integer> getPosition() {
        if (currentSegmentReader == null) {
            return new Tuple2<>(segments.size(), 0);
        }

        return new Tuple2<>(segmentIndex, currentSegmentReader.getOffset());
    }
}
