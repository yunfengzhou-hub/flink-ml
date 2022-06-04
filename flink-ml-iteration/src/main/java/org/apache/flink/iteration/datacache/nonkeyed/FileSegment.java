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

import org.apache.flink.core.fs.Path;

import java.util.Objects;

/** A segment contains the information about a cache unit. */
class FileSegment {

    private final Path path;

    /** The count of the records in the file. */
    private final int count;

    /** The total length of file. */
    private final long size;

    FileSegment(Path path, int count, long size) {
        this.path = path;
        this.count = count;
        this.size = size;
    }

    Path getPath() {
        return path;
    }

    int getCount() {
        return count;
    }

    long getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof FileSegment)) {
            return false;
        }

        FileSegment segment = (FileSegment) o;
        return count == segment.count && size == segment.size && Objects.equals(path, segment.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, count, size);
    }

    @Override
    public String toString() {
        return "Segment{" + "path=" + path + ", count=" + count + ", size=" + size + '}';
    }
}
