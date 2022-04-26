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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.function.SupplierWithException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Records the data received and replayed them on required. */
public class DataCacheWriter<T> {

    private final TypeSerializer<T> serializer;

    private final FileSystem fileSystem;

    private final SupplierWithException<Path, IOException> pathGenerator;

    private final List<Segment> finishSegments;

    private SegmentWriter currentSegment;

    public DataCacheWriter(
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator)
            throws IOException {
        this(serializer, fileSystem, pathGenerator, Collections.emptyList());
    }

    public DataCacheWriter(
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator,
            List<Segment> priorFinishedSegments)
            throws IOException {
        this.serializer = serializer;
        this.fileSystem = fileSystem;
        this.pathGenerator = pathGenerator;

        this.finishSegments = new ArrayList<>(priorFinishedSegments);

        this.currentSegment = new SegmentWriter(pathGenerator.get());
    }

    public void addRecord(T record) throws IOException {
        currentSegment.addRecord(record);
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
        if (currentSegment != null) {
            currentSegment.finish().ifPresent(finishSegments::add);
            currentSegment = null;
        }

        if (newSegment) {
            currentSegment = new SegmentWriter(pathGenerator.get());
        }
    }

    private class SegmentWriter {

        private final Path path;

        private final FSDataOutputStream outputStream;

        private final ObjectOutputStream objectOutputStream;

        private final ByteArrayOutputStream byteArrayOutputStream;

        private final DataOutputView outputView;

        private int currentSegmentCount;

        public SegmentWriter(Path path) throws IOException {
            this.path = path;
            this.outputStream = fileSystem.create(path, FileSystem.WriteMode.NO_OVERWRITE);
            this.objectOutputStream = new ObjectOutputStream(outputStream);

            this.byteArrayOutputStream = new ByteArrayOutputStream();
            this.outputView = new DataOutputViewStreamWrapper(byteArrayOutputStream);
        }

        public void addRecord(T record) throws IOException {
            serializer.serialize(record, outputView);
            objectOutputStream.writeObject(byteArrayOutputStream.toByteArray());
            byteArrayOutputStream.reset();
            currentSegmentCount += 1;
        }

        public Optional<Segment> finish() throws IOException {
            this.objectOutputStream.flush();
            this.outputStream.flush();
            long size = outputStream.getPos();
            this.outputStream.close();

            if (currentSegmentCount > 0) {
                return Optional.of(new Segment(path, currentSegmentCount, size));
            } else {
                // If there are no records, we tend to directly delete this file
                fileSystem.delete(path, false);
                return Optional.empty();
            }
        }
    }

    public void cleanup() throws IOException {
        finishCurrentSegment();
        for (Segment segment : finishSegments) {
            fileSystem.delete(segment.getPath(), false);
        }
        finishSegments.clear();
    }
}
