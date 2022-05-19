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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Optional;

/** A class that writes cache data to file system. */
@Internal
public class FsSegmentWriter<T> implements SegmentWriter<T> {
    private final FileSystem fileSystem;

    // TODO: adjust the file size limit automatically according to the provided file system.
    private static final long CACHE_FILE_SIZE_LIMIT = 100L * 1024L * 1024L; // 100MB

    // TODO: adjust the buffer size automatically to achieve best performance.
    private static final int STREAM_BUFFER_SIZE = 10 * 1024 * 1024; // 10MB

    private final TypeSerializer<T> serializer;

    private final Path path;

    private final FSDataOutputStream outputStream;

    private final ByteArrayOutputStream byteArrayOutputStream;

    private final ObjectOutputStream objectOutputStream;

    private final DataOutputView outputView;

    private int count;

    public FsSegmentWriter(TypeSerializer<T> serializer, Path path) throws IOException {
        this.serializer = serializer;
        this.path = path;
        this.fileSystem = path.getFileSystem();
        this.outputStream = fileSystem.create(path, FileSystem.WriteMode.NO_OVERWRITE);
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.objectOutputStream =
                new ObjectOutputStream(new BufferedOutputStream(outputStream, STREAM_BUFFER_SIZE));
        this.outputView = new DataOutputViewStreamWrapper(byteArrayOutputStream);
    }

    @Override
    public boolean addRecord(T record) {
        try {
            if (outputStream.getPos() > CACHE_FILE_SIZE_LIMIT) {
                return false;
            }
            // TODO: Simplify and improve performance of disk IO.
            serializer.serialize(record, outputView);
            objectOutputStream.writeObject(byteArrayOutputStream.toByteArray());
            byteArrayOutputStream.reset();

            count++;
            return true;
        } catch (IOException | RuntimeException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public Optional<Segment> finish() throws IOException {
        this.objectOutputStream.flush();
        this.outputStream.flush();
        long size = outputStream.getPos();
        this.outputStream.close();

        if (count > 0) {
            Segment segment = new Segment(path, count, size);
            return Optional.of(segment);
        } else {
            // If there are no records, we tend to directly delete this file
            fileSystem.delete(path, false);
            return Optional.empty();
        }
    }
}
