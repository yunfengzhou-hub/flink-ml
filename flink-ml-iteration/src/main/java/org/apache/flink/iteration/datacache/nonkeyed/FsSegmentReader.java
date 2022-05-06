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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/** A class that reads the cached data in a segment from file system. */
@Internal
class FsSegmentReader<T> implements SegmentReader<T> {
    // TODO: adjust the buffer size automatically to achieve best performance.
    private static final int STREAM_BUFFER_SIZE = 10 * 1024 * 1024; // 10MB

    private final TypeSerializer<T> serializer;

    private final int totalCount;

    private final InputStream inputStream;

    private final ObjectInputStream objectInputStream;

    private int offset;

    FsSegmentReader(TypeSerializer<T> serializer, Segment segment, int startOffset)
            throws IOException {
        this(
                serializer,
                segment.path.getFileSystem().open(segment.path),
                startOffset,
                segment.count);
    }

    FsSegmentReader(
            TypeSerializer<T> serializer, InputStream inputStream, int startOffset, int totalCount)
            throws IOException {
        this.serializer = serializer;
        this.inputStream = inputStream;
        this.objectInputStream =
                new ObjectInputStream(new BufferedInputStream(inputStream, STREAM_BUFFER_SIZE));
        this.offset = 0;
        this.totalCount = totalCount;

        for (int i = 0; i < startOffset; i++) {
            next();
        }
    }

    @Override
    public boolean hasNext() {
        return offset < totalCount;
    }

    @Override
    public T next() throws IOException {
        try {
            byte[] bytes = (byte[]) objectInputStream.readObject();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
            DataInputView tmpInputView = new DataInputViewStreamWrapper(inputStream);
            T value = serializer.deserialize(tmpInputView);
            offset++;
            return value;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public int getOffset() {
        return offset;
    }
}
