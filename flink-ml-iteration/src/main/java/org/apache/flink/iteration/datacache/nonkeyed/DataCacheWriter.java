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
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Records the data received and replays them on required. */
public class DataCacheWriter<T> {

    private final FileSystem fileSystem;

    private final SupplierWithException<Path, IOException> pathGenerator;

    private final MemoryManager memoryManager;

    private final TypeSerializer<T> serializer;

    private final List<Segment> finishSegments;

    private SegmentWriter<T> currentWriter;

    public DataCacheWriter(
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator,
            MemoryManager memoryManager,
            TypeSerializer<T> serializer)
            throws IOException {
        this(fileSystem, pathGenerator, memoryManager, serializer, Collections.emptyList());
    }

    public DataCacheWriter(
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator,
            MemoryManager memoryManager,
            TypeSerializer<T> serializer,
            List<Segment> priorFinishedSegments)
            throws IOException {
        this.serializer = serializer;
        this.fileSystem = fileSystem;
        this.pathGenerator = pathGenerator;
        this.memoryManager = memoryManager;
        this.finishSegments = new ArrayList<>(priorFinishedSegments);
        this.currentWriter = createSegmentWriter(pathGenerator, this.memoryManager);
    }

    public void addRecord(T record) throws IOException {
        boolean success = currentWriter.addRecord(record);
        if (!success) {
            finishCurrentSegment();
            success = currentWriter.addRecord(record);
            Preconditions.checkState(success);
        }
    }

    public void finishCurrentSegment() throws IOException {
        if (currentWriter != null) {
            currentWriter.finish().ifPresent(finishSegments::add);
            currentWriter = null;
        }

        currentWriter = createSegmentWriter(pathGenerator, memoryManager);
    }

    public List<Segment> finish() throws IOException {
        if (currentWriter != null) {
            currentWriter.finish().ifPresent(finishSegments::add);
            currentWriter = null;
        }
        return finishSegments;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public List<Segment> getFinishSegments() {
        return finishSegments;
    }

    private SegmentWriter<T> createSegmentWriter(
            SupplierWithException<Path, IOException> pathGenerator, MemoryManager memoryManager)
            throws IOException {
        boolean shouldCacheInMemory = MemoryUtils.isMemoryEnoughForCache(memoryManager);

        if (shouldCacheInMemory) {
            try {
                return new MemorySegmentWriter<>(pathGenerator.get(), memoryManager);
            } catch (MemoryAllocationException e) {
                return new FsSegmentWriter<>(serializer, pathGenerator.get());
            }
        }
        return new FsSegmentWriter<>(serializer, pathGenerator.get());
    }

    public void cleanup() throws IOException {
        finish();
        for (Segment segment : finishSegments) {
            if (segment.isOnDisk()) {
                fileSystem.delete(segment.path, false);
            }
            if (segment.isInMemory()) {
                memoryManager.releaseMemory(segment.path, segment.inMemorySize);
            }
        }
        finishSegments.clear();
    }
}
