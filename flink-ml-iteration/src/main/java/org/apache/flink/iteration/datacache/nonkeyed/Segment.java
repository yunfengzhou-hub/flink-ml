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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** A segment contains the information about a cache unit. */
public class Segment implements Serializable {

    /** The pre-allocated path on disk to persist the records. */
    Path path;

    /** The number of records in the file. */
    int count;

    /** The size of the records in file. */
    long fsSize;

    /** The size of the records in memory. */
    transient long inMemorySize;

    /** The cached records in memory. */
    transient List<Object> cache;

    /** The serializer for the records. */
    transient TypeSerializer<Object> serializer;

    Segment() {}

    Segment(Path path, int count, long fsSize) {
        this.path = path;
        this.count = count;
        this.fsSize = fsSize;
    }

    boolean isOnDisk() throws IOException {
        return path.getFileSystem().exists(path);
    }

    boolean isInMemory() {
        return cache != null;
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
                && Objects.equals(path, segment.path)
                && Objects.equals(fsSize, segment.fsSize)
                && Objects.equals(inMemorySize, segment.inMemorySize)
                && Objects.equals(cache, segment.cache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, count, fsSize, inMemorySize, cache);
    }

    @Override
    public String toString() {
        return String.format(
                "Segment{path=%s, count=%d, isInMemory=%b}", path, count, isInMemory());
    }
}
