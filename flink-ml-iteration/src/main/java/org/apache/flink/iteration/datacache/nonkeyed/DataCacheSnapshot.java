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
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.util.NonClosingInputStreamDecorator;
import org.apache.flink.runtime.util.NonClosingOutputStreamDecorator;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.io.input.BoundedInputStream;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** The snapshot of a data cache. It could be written out or read from an external stream.O */
public class DataCacheSnapshot {

    private static final int CURRENT_VERSION = 1;

    private final FileSystem fileSystem;

    @Nullable private final Tuple2<Integer, Integer> readerPosition;

    private final List<Segment> segments;

    public DataCacheSnapshot(
            FileSystem fileSystem,
            @Nullable Tuple2<Integer, Integer> readerPosition,
            List<Segment> segments) {
        this.fileSystem = fileSystem;
        this.readerPosition = readerPosition;
        for (Segment segment : segments) {
            Preconditions.checkArgument(segment.isOnDisk());
        }
        this.segments = segments;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    @Nullable
    public Tuple2<Integer, Integer> getReaderPosition() {
        return readerPosition;
    }

    public List<Segment> getSegments() {
        return segments;
    }

    /** Writes the information about this data cache to an output stream. */
    public void writeTo(OutputStream outputStream) throws IOException {
        try (DataOutputStream dos =
                new DataOutputStream(new NonClosingOutputStreamDecorator(outputStream))) {
            dos.writeInt(CURRENT_VERSION);
            dos.writeBoolean(readerPosition != null);
            if (readerPosition != null) {
                dos.writeInt(readerPosition.f0);
                dos.writeInt(readerPosition.f1);
            }

            dos.writeBoolean(fileSystem.isDistributedFS());
            if (fileSystem.isDistributedFS()) {
                // We only need to record the segments itself
                serializeSegments(segments, dos);
            } else {
                // We have to copy the whole streams.
                dos.writeInt(segments.size());
                for (Segment segment : segments) {
                    dos.writeInt(segment.getCount());
                    dos.writeLong(segment.getFsSize());
                    try (FSDataInputStream inputStream = fileSystem.open(segment.getPath())) {
                        IOUtils.copyBytes(inputStream, outputStream, false);
                    }
                }
            }
        }
    }

    /**
     * Replays cached records in the data cache from input stream into the target feedback consumer.
     */
    public static <T> void replay(
            InputStream inputStream,
            TypeSerializer<T> serializer,
            FeedbackConsumer<T> feedbackConsumer)
            throws Exception {
        try (DataInputStream dis =
                new DataInputStream(new NonClosingInputStreamDecorator(inputStream))) {
            int version = dis.readInt();
            checkState(
                    version == CURRENT_VERSION,
                    "Currently only support version " + CURRENT_VERSION);
            parseReaderPosition(dis);

            boolean isDistributedFS = dis.readBoolean();
            if (isDistributedFS) {
                List<Segment> segments = deserializeSegments(dis);

                DataCacheReader<T> dataCacheReader = new DataCacheReader<>(serializer, segments);

                while (dataCacheReader.hasNext()) {
                    T t = dataCacheReader.next();
                    feedbackConsumer.processFeedback(t);
                }
            } else {
                DataInputViewStreamWrapper dataInputView = new DataInputViewStreamWrapper(dis);
                int segmentNum = dis.readInt();
                for (int i = 0; i < segmentNum; i++) {
                    int count = dis.readInt();
                    dis.readLong();
                    for (int j = 0; j < count; j++) {
                        feedbackConsumer.processFeedback(serializer.deserialize(dataInputView));
                    }
                }
            }
        }
    }

    /** Recovers a data cache instance from the input stream. */
    public static DataCacheSnapshot recover(
            InputStream inputStream,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator)
            throws IOException {
        try (DataInputStream dis =
                new DataInputStream(new NonClosingInputStreamDecorator(inputStream))) {
            int version = dis.readInt();
            checkState(
                    version == CURRENT_VERSION,
                    "Currently only support version " + CURRENT_VERSION);
            Tuple2<Integer, Integer> readerPosition = parseReaderPosition(dis);

            boolean isDistributedFS = dis.readBoolean();
            checkState(
                    isDistributedFS == fileSystem.isDistributedFS(),
                    "Currently we do not support changing the cache file system. "
                            + "If required, please manually copy the directory from one filesystem to another.");

            List<Segment> segments;
            if (isDistributedFS) {
                segments = deserializeSegments(dis);
            } else {
                int segmentNum = dis.readInt();
                segments = new ArrayList<>(segmentNum);
                for (int i = 0; i < segmentNum; i++) {
                    int count = dis.readInt();
                    long fsSize = dis.readLong();
                    Path path = pathGenerator.get();
                    try (FSDataOutputStream outputStream =
                            fileSystem.create(path, FileSystem.WriteMode.NO_OVERWRITE)) {

                        BoundedInputStream boundedInputStream =
                                new BoundedInputStream(inputStream, fsSize);
                        boundedInputStream.setPropagateClose(false);
                        IOUtils.copyBytes(boundedInputStream, outputStream, false);
                        boundedInputStream.close();
                    }
                    segments.add(new Segment(path, count, fsSize));
                }
            }

            return new DataCacheSnapshot(fileSystem, readerPosition, segments);
        }
    }

    public <T> void tryCacheToMemory(TypeSerializer<T> serializer, MemorySegmentPool segmentPool)
            throws IOException {
        Preconditions.checkNotNull(segmentPool);
        for (Segment segment : segments) {
            if (segment.isCached()) {
                continue;
            }

            SegmentReader<T> reader = new FileSegmentReader<>(serializer, segment, 0);
            SegmentWriter<T> writer;
            try {
                writer =
                        new MemorySegmentWriter<>(
                                serializer, segment.getPath(), segmentPool, segment.getFsSize());
                while (reader.hasNext()) {
                    writer.addRecord(reader.next());
                }
                writer.finish().ifPresent(x -> segment.setCache(x.getCache()));
            } catch (IOException ignored) {
                // Ignore exception if there is no enough memory space for cache.
            }
        }
    }

    private static Tuple2<Integer, Integer> parseReaderPosition(DataInputStream dataInputStream)
            throws IOException {
        Tuple2<Integer, Integer> readerPosition = null;
        boolean hasReaderPosition = dataInputStream.readBoolean();
        if (hasReaderPosition) {
            readerPosition = new Tuple2<>(dataInputStream.readInt(), dataInputStream.readInt());
        }

        return readerPosition;
    }

    private static void serializeSegments(List<Segment> segments, DataOutputStream dataOutputStream)
            throws IOException {
        dataOutputStream.writeInt(segments.size());
        for (Segment segment : segments) {
            dataOutputStream.writeUTF(segment.getPath().toString());
            dataOutputStream.writeInt(segment.getCount());
            dataOutputStream.writeLong(segment.getFsSize());
        }
    }

    private static List<Segment> deserializeSegments(DataInputStream dataInputStream)
            throws IOException {
        List<Segment> segments = new ArrayList<>();
        int numberOfSegments = dataInputStream.readInt();
        for (int i = 0; i < numberOfSegments; ++i) {
            segments.add(
                    new Segment(
                            new Path(dataInputStream.readUTF()),
                            dataInputStream.readInt(),
                            dataInputStream.readLong()));
        }
        return segments;
    }
}
