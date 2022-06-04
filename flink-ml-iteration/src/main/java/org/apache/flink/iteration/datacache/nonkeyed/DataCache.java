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
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.io.input.BoundedInputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** Records the data received and replayed them on required. */
public class DataCache<T> implements Iterable<T> {

    private static final int CURRENT_VERSION = 1;

    private final FileSystem fileSystem;

    private final SupplierWithException<Path, IOException> pathGenerator;

    private final MemorySegmentPool segmentPool;

    private final TypeSerializer<T> serializer;

    private final List<Segment> finishedSegments;

    private SegmentWriter<T> currentWriter;

    public DataCache(
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator)
            throws IOException {
        this(serializer, fileSystem, pathGenerator, null, Collections.emptyList());
    }

    public DataCache(
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator,
            MemorySegmentPool segmentPool)
            throws IOException {
        this(serializer, fileSystem, pathGenerator, segmentPool, Collections.emptyList());
    }

    DataCache(
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator,
            MemorySegmentPool segmentPool,
            List<Segment> finishedSegments)
            throws IOException {
        this.fileSystem = fileSystem;
        this.pathGenerator = pathGenerator;
        this.segmentPool = segmentPool;
        this.serializer = serializer;
        this.finishedSegments = new ArrayList<>();
        this.finishedSegments.addAll(finishedSegments);
        for (Segment segment : finishedSegments) {
            tryCacheSegmentToMemory(segment);
        }
        this.currentWriter = createSegmentWriter();
    }

    public void addRecord(T record) throws IOException {
        try {
            currentWriter.addRecord(record);
        } catch (SegmentNoVacancyException e) {
            currentWriter.finish().ifPresent(finishedSegments::add);
            currentWriter = new FileSegmentWriter<>(serializer, pathGenerator.get());
            currentWriter.addRecord(record);
        }
    }

    public void finish() throws IOException {
        if (currentWriter == null) {
            return;
        }

        currentWriter.finish().ifPresent(finishedSegments::add);
        currentWriter = null;
    }

    private void finishCurrentSegmentIfAny() throws IOException {
        if (currentWriter == null || currentWriter.getCount() == 0) {
            return;
        }

        currentWriter.finish().ifPresent(finishedSegments::add);
        currentWriter = createSegmentWriter();
    }

    public void cleanup() throws IOException {
        finishCurrentSegmentIfAny();
        for (Segment segment : finishedSegments) {
            if (segment.getFileSegment() != null) {
                fileSystem.delete(segment.getFileSegment().getPath(), false);
            }
            if (segment.getMemorySegment() != null) {
                segmentPool.returnAll(segment.getMemorySegment().getCache());
            }
        }
        finishedSegments.clear();
    }

    @Override
    public DataCacheIterator<T> iterator() {
        try {
            finishCurrentSegmentIfAny();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new DataCacheIterator<>(serializer, finishedSegments);
    }

    public void writeTo(OutputStream checkpointOutputStream) throws IOException {
        finishCurrentSegmentIfAny();
        try (DataOutputStream dos =
                new DataOutputStream(new NonClosingOutputStreamDecorator(checkpointOutputStream))) {
            dos.writeInt(CURRENT_VERSION);

            dos.writeBoolean(fileSystem.isDistributedFS());
            for (Segment segment : finishedSegments) {
                persistSegmentToDisk(segment);
            }
            if (fileSystem.isDistributedFS()) {
                // We only need to record the segments itself
                serializeSegments(finishedSegments, dos);
            } else {
                // We have to copy the whole streams.
                dos.writeInt(finishedSegments.size());
                for (Segment segment : finishedSegments) {
                    FileSegment fileSegment = segment.getFileSegment();

                    dos.writeInt(fileSegment.getCount());
                    dos.writeLong(fileSegment.getSize());
                    try (FSDataInputStream inputStream = fileSystem.open(fileSegment.getPath())) {
                        IOUtils.copyBytes(inputStream, checkpointOutputStream, false);
                    }
                }
            }
        }
    }

    public static <T> void replay(
            InputStream checkpointInputStream,
            TypeSerializer<T> serializer,
            FeedbackConsumer<T> feedbackConsumer)
            throws Exception {
        try (DataInputStream dis =
                new DataInputStream(new NonClosingInputStreamDecorator(checkpointInputStream))) {
            int version = dis.readInt();
            checkState(
                    version == CURRENT_VERSION,
                    "Currently only support version " + CURRENT_VERSION);

            boolean isDistributedFS = dis.readBoolean();
            if (isDistributedFS) {
                List<Segment> segments = deserializeSegments(dis);

                DataCacheIterator<T> dataCacheIterator =
                        new DataCacheIterator<>(serializer, segments);

                while (dataCacheIterator.hasNext()) {
                    T t = dataCacheIterator.next();
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

    public static <T> DataCache<T> recover(
            InputStream checkpointInputStream,
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator)
            throws IOException {
        return recover(checkpointInputStream, serializer, fileSystem, pathGenerator, null);
    }

    public static <T> DataCache<T> recover(
            InputStream checkpointInputStream,
            TypeSerializer<T> serializer,
            FileSystem fileSystem,
            SupplierWithException<Path, IOException> pathGenerator,
            MemorySegmentPool memoryManager)
            throws IOException {
        try (DataInputStream dis =
                new DataInputStream(new NonClosingInputStreamDecorator(checkpointInputStream))) {
            int version = dis.readInt();
            checkState(
                    version == CURRENT_VERSION,
                    "Currently only support version " + CURRENT_VERSION);

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

                        BoundedInputStream inputStream =
                                new BoundedInputStream(checkpointInputStream, fsSize);
                        inputStream.setPropagateClose(false);
                        IOUtils.copyBytes(inputStream, outputStream, false);
                        inputStream.close();
                    }
                    segments.add(new Segment(new FileSegment(path, count, fsSize)));
                }
            }

            return new DataCache<>(serializer, fileSystem, pathGenerator, memoryManager, segments);
        }
    }

    private SegmentWriter<T> createSegmentWriter() throws IOException {
        if (segmentPool != null) {
            try {
                return new MemorySegmentWriter<>(serializer, segmentPool, 0L);
            } catch (SegmentNoVacancyException e) {
                // ignore MemoryAllocationException.
            }
        }
        return new FileSegmentWriter<>(serializer, pathGenerator.get());
    }

    private static void serializeSegments(List<Segment> segments, DataOutputStream dataOutputStream)
            throws IOException {
        dataOutputStream.writeInt(segments.size());
        for (Segment segment : segments) {
            FileSegment fileSegment = segment.getFileSegment();
            dataOutputStream.writeUTF(fileSegment.getPath().toString());
            dataOutputStream.writeInt(fileSegment.getCount());
            dataOutputStream.writeLong(fileSegment.getSize());
        }
    }

    private static List<Segment> deserializeSegments(DataInputStream dataInputStream)
            throws IOException {
        List<Segment> segments = new ArrayList<>();
        int numberOfSegments = dataInputStream.readInt();
        for (int i = 0; i < numberOfSegments; ++i) {
            segments.add(
                    new Segment(
                            new FileSegment(
                                    new Path(dataInputStream.readUTF()),
                                    dataInputStream.readInt(),
                                    dataInputStream.readLong())));
        }
        return segments;
    }

    private void persistSegmentToDisk(Segment segment) throws IOException {
        if (segment.getFileSegment() != null) {
            return;
        }

        SegmentReader<T> reader = new MemorySegmentReader<>(serializer, segment, 0);
        SegmentWriter<T> writer = new FileSegmentWriter<>(serializer, pathGenerator.get());
        while (reader.hasNext()) {
            writer.addRecord(reader.next());
        }
        writer.finish().ifPresent(x -> segment.setFileSegment(x.getFileSegment()));
    }

    private void tryCacheSegmentToMemory(Segment segment) throws IOException {
        if (segment.getMemorySegment() != null || segmentPool == null) {
            return;
        }

        SegmentReader<T> reader = new FileSegmentReader<>(serializer, segment, 0);
        SegmentWriter<T> writer;
        try {
            writer =
                    new MemorySegmentWriter<>(
                            serializer, segmentPool, segment.getFileSegment().getSize());
            while (reader.hasNext()) {
                writer.addRecord(reader.next());
            }
            writer.finish().ifPresent(x -> segment.setFileSegment(x.getFileSegment()));
        } catch (SegmentNoVacancyException ignored) {
            // Ignore exception if there is no enough memory space for cache.
        }
    }
}
