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
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** A segment contains the information about a cache unit. */
public class Segment implements Serializable {

    /** The number of records in the file. */
    private final int count;

    private FsSegment fsSegment;

    private transient MemorySegment memorySegment;

    Segment(Path path, int count, long fsSize) {
        this.count = count;
        this.fsSegment = new FsSegment(path, fsSize);
    }

    Segment(
            int count,
            Path path,
            List<Object> cache,
            long inMemorySize,
            TypeSerializer<Object> serializer) {
        this.count = count;
        this.memorySegment = new MemorySegment(cache, inMemorySize, serializer, path);
    }

    int getCount() {
        return count;
    }

    void setFsSegment(Path path, long fsSize) {
        Preconditions.checkState(fsSegment == null);
        Preconditions.checkNotNull(memorySegment);
        Preconditions.checkArgument(memorySegment.path.equals(path));
        fsSegment = new FsSegment(path, fsSize);
    }

    FsSegment getFsSegment() {
        return fsSegment;
    }

    void setMemorySegment(MemorySegment segment) {
        Preconditions.checkState(memorySegment == null);
        Preconditions.checkNotNull(fsSegment);
        Preconditions.checkArgument(fsSegment.path.equals(segment.path));
        this.memorySegment = segment;
    }

    MemorySegment getMemorySegment() {
        return memorySegment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Segment)) {
            return false;
        }

        Segment segment = (Segment) o;
        return count == segment.count
                && Objects.equals(fsSegment, segment.fsSegment)
                && Objects.equals(memorySegment, segment.memorySegment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, fsSegment, memorySegment);
    }

    static class FsSegment implements Serializable {
        /** The pre-allocated path on disk to persist the records. */
        private final Path path;

        /** The size of the records in file. */
        private final long size;

        private FsSegment(Path path, long size) {
            this.path = path;
            this.size = size;
        }

        Path getPath() {
            return path;
        }

        long getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof FsSegment)) {
                return false;
            }

            FsSegment segment = (FsSegment) o;
            return Objects.equals(path, segment.path) && Objects.equals(size, segment.size);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, size);
        }
    }

    static class MemorySegment {
        /** The cached records in memory. */
        private final List<Object> cache;

        /** The size of the records in memory. */
        private final long size;

        /** The serializer for the records. */
        private final TypeSerializer<Object> serializer;

        /** The pre-allocated path on disk to persist the records. */
        private final Path path;

        private MemorySegment(
                List<Object> cache, long size, TypeSerializer<Object> serializer, Path path) {
            this.cache = cache;
            this.size = size;
            this.serializer = serializer;
            this.path = path;
        }

        @SuppressWarnings("unchecked")
        <T> List<T> getCache() {
            return (List<T>) cache;
        }

        long getSize() {
            return size;
        }

        @SuppressWarnings("unchecked")
        <T> TypeSerializer<T> getTypeSerializer() {
            return (TypeSerializer<T>) serializer;
        }

        Path getPath() {
            return path;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof MemorySegment)) {
                return false;
            }

            MemorySegment segment = (MemorySegment) o;
            return Objects.equals(path, segment.path)
                    && Objects.equals(size, segment.size)
                    && Objects.equals(cache, segment.cache)
                    && Objects.equals(serializer, segment.serializer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, size, cache, serializer);
        }
    }
}
