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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * A segment contains the information about a cache unit.
 *
 * <p>If the unit is persisted in a file on disk, this class provides the number of records in the
 * unit, the path to the file, and the size of the file.
 *
 * <p>If the unit is cached in memory, this class provides the number of records, the cached
 * objects, and information to persist them on disk, including the pre-allocated path, and the type
 * serializer.
 */
@Internal
public class Segment implements Serializable {

    /** The pre-allocated path to persist records on disk. */
    private final Path path;

    /** The number of records in the segment. */
    private final int count;

    /** The size of the file containing cached records. */
    private long fsSize;

    private transient Object cacheKey;

    /** The cached records. */
    private transient List<MemorySegment> cache;

    private transient TypeSerializer<Object> serializer;

    Segment(Path path, int count, long fsSize) {
        Preconditions.checkNotNull(path);
        Preconditions.checkArgument(count > 0);
        Preconditions.checkArgument(fsSize > 0);
        this.path = path;
        this.count = count;
        this.fsSize = fsSize;
    }

    Segment(
            Path path,
            int count,
            Object cacheKey,
            List<MemorySegment> cache,
            TypeSerializer<Object> serializer) {
        Preconditions.checkNotNull(path);
        Preconditions.checkArgument(count > 0);
        Preconditions.checkNotNull(cache);
        this.path = path;
        this.count = count;
        this.cacheKey = cacheKey;
        this.cache = cache;
        this.serializer = serializer;
        this.fsSize = -1;
    }

    boolean isOnDisk() {
        return fsSize > 0;
    }

    void setDiskInfo(long fsSize) {
        Preconditions.checkState(!isOnDisk());
        Preconditions.checkArgument(fsSize > 0);
        this.fsSize = fsSize;
    }

    boolean isCached() {
        return cache != null;
    }

    void setCacheInfo(
            Object cacheKey, List<MemorySegment> cache, TypeSerializer<Object> serializer) {
        Preconditions.checkState(!isCached());
        Preconditions.checkNotNull(cache);
        this.cacheKey = cacheKey;
        this.cache = cache;
        this.serializer = serializer;
    }

    int getCount() {
        return count;
    }

    Path getPath() {
        return path;
    }

    long getFsSize() {
        return fsSize;
    }

    Object getCacheKey() {
        return cacheKey;
    }

    List<MemorySegment> getCache() {
        return cache;
    }

    @SuppressWarnings("unchecked")
    <T> TypeSerializer<T> getTypeSerializer() {
        return (TypeSerializer<T>) serializer;
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
        return count == segment.count && Objects.equals(path, segment.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, path);
    }
}
