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

import java.io.IOException;
import java.util.Optional;

/** Writer for the data to be cached to a segment. */
@Internal
interface SegmentWriter<T> {
    /** Adds a record to the writer. */
    boolean addRecord(T record);

    /** Gets the number of records added so far. */
    int getCount();

    /**
     * Finishes the writer and returns a segment if any record has ever been added through {@link
     * #addRecord(Object)}.
     */
    Optional<Segment> finish() throws IOException;

    static <T> SegmentWriter<T> create(
            Path path,
            MemoryManager memoryManager,
            TypeSerializer<T> serializer,
            long expectedSize,
            boolean tryCacheInMemory,
            boolean createFsWriterIfMemoryException)
            throws IOException {
        if (tryCacheInMemory) {
            try {
                return new MemorySegmentWriter<>(path, memoryManager, serializer, expectedSize);
            } catch (MemoryReservationException e) {
                if (createFsWriterIfMemoryException) {
                    return new FsSegmentWriter<>(serializer, path);
                }
                return null;
            }
        }
        return new FsSegmentWriter<>(serializer, path);
    }
}
