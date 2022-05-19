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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryReservationException;
import org.apache.flink.util.Preconditions;

import org.openjdk.jol.info.GraphLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** A class that writes cache data to memory segments. */
@Internal
public class MemorySegmentWriter<T> implements SegmentWriter<T> {
    private final MemoryManager memoryManager;

    private final Path path;

    private final List<T> cache;

    private final TypeSerializer<T> serializer;

    private long inMemorySize;

    private int count;

    private long reservedMemorySize;

    public MemorySegmentWriter(
            Path path, MemoryManager memoryManager, TypeSerializer<T> serializer) {
        this.serializer = serializer;
        Preconditions.checkNotNull(memoryManager);
        this.path = path;
        this.cache = new ArrayList<>();
        this.inMemorySize = 0L;
        this.count = 0;
        this.memoryManager = memoryManager;
        this.reservedMemorySize = 0L;
    }

    public MemorySegmentWriter(
            Path path, MemoryManager memoryManager, TypeSerializer<T> serializer, long expectedSize)
            throws MemoryReservationException {
        this.serializer = serializer;
        Preconditions.checkNotNull(memoryManager);
        this.path = path;
        this.cache = new ArrayList<>();
        this.inMemorySize = 0L;
        this.count = 0;
        this.memoryManager = memoryManager;

        if (expectedSize > 0) {
            memoryManager.reserveMemory(this.path, expectedSize);
        }
        this.reservedMemorySize = expectedSize;
    }

    @Override
    public boolean addRecord(T record) {
        if (!MemoryUtils.isMemoryEnoughForCache(memoryManager)) {
            return false;
        }

        long recordSize = GraphLayout.parseInstance(record).totalSize();

        try {
            long memorySizeToReserve = inMemorySize + recordSize - reservedMemorySize;
            if (memorySizeToReserve > 0) {
                memoryManager.reserveMemory(this.path, memorySizeToReserve);
                reservedMemorySize += memorySizeToReserve;
            }
        } catch (MemoryReservationException e) {
            return false;
        }

        this.cache.add(record);
        inMemorySize += recordSize;

        this.count++;
        return true;
    }

    @Override
    public int getCount() {
        return this.count;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<Segment> finish() throws IOException {
        if (count > 0) {
            return Optional.of(
                    new Segment(
                            count,
                            path,
                            (List<Object>) cache,
                            inMemorySize,
                            (TypeSerializer<Object>) serializer));
        } else {
            memoryManager.releaseMemory(path, inMemorySize);
            return Optional.empty();
        }
    }
}
