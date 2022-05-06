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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryReservationException;
import org.apache.flink.util.Preconditions;

import org.openjdk.jol.info.GraphLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;

/** A class that writes cache data to memory segments. */
@Internal
public class MemorySegmentWriter<T> implements SegmentWriter<T> {
    private final Segment segment;

    private final MemoryManager memoryManager;

    public MemorySegmentWriter(Path path, MemoryManager memoryManager)
            throws MemoryAllocationException {
        this(path, memoryManager, 0L);
    }

    public MemorySegmentWriter(Path path, MemoryManager memoryManager, long expectedSize)
            throws MemoryAllocationException {
        Preconditions.checkNotNull(memoryManager);
        this.segment = new Segment();
        this.segment.path = path;
        this.segment.cache = new ArrayList<>();
        this.segment.inMemorySize = 0L;
        this.memoryManager = memoryManager;
    }

    @Override
    public boolean addRecord(T record) throws IOException {
        if (!MemoryUtils.isMemoryEnoughForCache(memoryManager)) {
            return false;
        }

        long recordSize = GraphLayout.parseInstance(record).totalSize();

        try {
            memoryManager.reserveMemory(this, recordSize);
        } catch (MemoryReservationException e) {
            return false;
        }

        this.segment.cache.add(record);
        segment.inMemorySize += recordSize;

        this.segment.count++;
        return true;
    }

    @Override
    public Optional<Segment> finish() throws IOException {
        if (segment.count > 0) {
            return Optional.of(segment);
        } else {
            memoryManager.releaseMemory(segment.path, segment.inMemorySize);
            return Optional.empty();
        }
    }
}
