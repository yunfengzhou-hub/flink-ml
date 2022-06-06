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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Records the data received and replayed them on required. */
public class DataCacheWriter<T> {

    static final long MAX_SEGMENT_SIZE = 1L << 30; // 1GB

    private final TypeSerializer<T> serializer;

    private final FileSystem fileSystem;

    private final SupplierWithException<Path, IOException> pathGenerator;

    @Nullable private final MemorySegmentPool segmentPool;

    private final List<Segment> finishedSegments;

    private SegmentWriter<T> currentWriter;

    public DataCacheWriter(
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator)
            throws IOException {
        this(serializer, fileSystem, pathGenerator, null, Collections.emptyList());
    }

    public DataCacheWriter(
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator,
            MemorySegmentPool segmentPool)
            throws IOException {
        this(serializer, fileSystem, pathGenerator, segmentPool, Collections.emptyList());
    }

    public DataCacheWriter(
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator,
            List<Segment> finishedSegments)
            throws IOException {
        this(serializer, fileSystem, pathGenerator, null, finishedSegments);
    }

    public DataCacheWriter(
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator,
            @Nullable MemorySegmentPool segmentPool,
            List<Segment> finishedSegments)
            throws IOException {
        this.fileSystem = fileSystem;
        this.pathGenerator = pathGenerator;
        this.segmentPool = segmentPool;
        this.serializer = serializer;
        this.finishedSegments = new ArrayList<>();
        this.finishedSegments.addAll(finishedSegments);
        this.currentWriter = createSegmentWriter();
    }

    public void addRecord(T record) throws IOException {
        if (!currentWriter.addRecord(record)) {
            currentWriter.finish().ifPresent(finishedSegments::add);
            currentWriter = new FileSegmentWriter<>(serializer, pathGenerator.get());
            currentWriter.addRecord(record);
        }
    }

    /** Finishes current segment if records has ever been added to this segment. */
    public void finishCurrentSegmentIfAny() throws IOException {
        if (currentWriter == null || currentWriter.getCount() == 0) {
            return;
        }

        currentWriter.finish().ifPresent(finishedSegments::add);
        currentWriter = createSegmentWriter();
    }

    /** Finishes adding records and closes resources occupied for adding records. */
    public List<Segment> finish() throws IOException {
        if (currentWriter == null) {
            return finishedSegments;
        }

        currentWriter.finish().ifPresent(finishedSegments::add);
        currentWriter = null;
        return finishedSegments;
    }

    public List<Segment> getFinishedSegments() {
        return finishedSegments;
    }

    /** Cleans up all previously added records. */
    public void cleanup() throws IOException {
        finishCurrentSegmentIfAny();
        for (Segment segment : finishedSegments) {
            if (segment.isOnDisk()) {
                fileSystem.delete(segment.getPath(), false);
            }
            if (segment.isCached()) {
                segmentPool.returnAll(segment.getCache());
            }
        }
        finishedSegments.clear();
    }

    public void persistToDisk() throws IOException {
        for (Segment segment : finishedSegments) {
            if (segment.isOnDisk()) {
                continue;
            }

            SegmentReader<T> reader = new MemorySegmentReader<>(serializer, segment, 0);
            SegmentWriter<T> writer = new FileSegmentWriter<>(serializer, segment.getPath());
            while (reader.hasNext()) {
                writer.addRecord(reader.next());
            }
            writer.finish().ifPresent(x -> segment.setDiskInfo(x.getFsSize()));
        }
    }

    private SegmentWriter<T> createSegmentWriter() throws IOException {
        if (segmentPool != null) {
            try {
                return new MemorySegmentWriter<>(serializer, pathGenerator.get(), segmentPool, 0L);
            } catch (IOException ignored) {
                // ignore MemoryAllocationException.
            }
        }
        return new FileSegmentWriter<>(serializer, pathGenerator.get());
    }
}
