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
import java.io.IOException;
import java.util.Optional;

/** A class that writes cache data to file system. */
@Internal
public class FileSegmentWriter<T> implements SegmentWriter<T> {

    private static final long FILE_SIZE_LIMIT = 1L << 30; // 1GB

    private final FileSystem fileSystem;

    private final TypeSerializer<T> serializer;

    private final Path path;

    private final FSDataOutputStream outputStream;

    private final BufferedOutputStream bufferedOutputStream;

    private final DataOutputView outputView;

    private int count;

    public FileSegmentWriter(TypeSerializer<T> serializer, Path path) throws IOException {
        this.serializer = serializer;
        this.path = path;
        this.fileSystem = path.getFileSystem();
        this.outputStream = fileSystem.create(path, FileSystem.WriteMode.NO_OVERWRITE);
        this.bufferedOutputStream = new BufferedOutputStream(outputStream);
        this.outputView = new DataOutputViewStreamWrapper(bufferedOutputStream);
    }

    @Override
    public void addRecord(T record) throws IOException {
        if (outputStream.getPos() >= FILE_SIZE_LIMIT) {
            throw new SegmentNoVacancyException();
        }
        serializer.serialize(record, outputView);
        count++;
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public Optional<Segment> finish() throws IOException {
        this.bufferedOutputStream.flush();
        this.outputStream.flush();
        long size = outputStream.getPos();
        this.outputStream.close();

        if (count > 0) {
            Segment segment = new Segment(new FileSegment(path, count, size));
            return Optional.of(segment);
        } else {
            // If there are no records, we tend to directly delete this file
            fileSystem.delete(path, false);
            return Optional.empty();
        }
    }
}
