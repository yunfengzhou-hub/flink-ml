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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.function.SupplierWithException;

import java.io.ByteArrayOutputStream;
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

    private SegmentWriter currentWriter;

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

        this.currentWriter = createSegmentWriter(pathGenerator, memoryManager);
    }

    public boolean addRecord(T record) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputView outputView = new DataOutputViewStreamWrapper(byteArrayOutputStream);
        serializer.serialize(record, outputView);
        byte[] bytes = byteArrayOutputStream.toByteArray();

        return currentWriter.addRecord(bytes);
    }

    public void finishCurrentSegment() throws IOException {
        finishCurrentSegment(true);
    }

    public List<Segment> finish() throws IOException {
        finishCurrentSegment(false);
        return finishSegments;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public List<Segment> getFinishSegments() {
        return finishSegments;
    }

    private void finishCurrentSegment(boolean newSegment) throws IOException {
        if (currentWriter != null) {
            currentWriter.finish().ifPresent(finishSegments::add);
            currentWriter = null;
        }

        if (newSegment) {
            currentWriter = createSegmentWriter(pathGenerator, memoryManager);
        }
    }

    private static SegmentWriter createSegmentWriter(
            SupplierWithException<Path, IOException> pathGenerator, MemoryManager memoryManager)
            throws IOException {
        boolean shouldCacheInMemory =
                memoryManager != null
                        && (double) memoryManager.availableMemory() / memoryManager.getMemorySize()
                                > 0.5;

        if (shouldCacheInMemory) {
            return new MemorySegmentWriter(pathGenerator.get(), memoryManager);
        }
        return new FsSegmentWriter(pathGenerator.get());
    }

    public void cleanup() throws IOException {
        finishCurrentSegment();
        for (Segment segment : finishSegments) {
            fileSystem.delete(segment.getPath(), false);
        }
        finishSegments.clear();
    }
}
