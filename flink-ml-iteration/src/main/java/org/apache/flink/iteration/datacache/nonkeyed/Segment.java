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
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** A segment represents a single file for the cache. */
public class Segment implements Serializable {

    private final Path path;

    /** The count of the records in the file. */
    private final int count;

    private transient List<MemorySegment> bufferedMemorySegment;

    public Segment(Path path, int count) {
        this.path = path;
        this.count = count;
    }

    public boolean isOnDisk() throws IOException {
        return path.getFileSystem().exists(path);
    }

    public Path getPath() {
        return path;
    }

    public int getCount() {
        return count;
    }

    public boolean isInMemory() {
        return bufferedMemorySegment != null;
    }

    public List<MemorySegment> getBufferedMemorySegment() {
        return bufferedMemorySegment;
    }

    public void setBufferedMemorySegments(List<MemorySegment> bufferedMemorySegment) {
        this.bufferedMemorySegment = bufferedMemorySegment;
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
        return Objects.hash(path, count);
    }

    @Override
    public String toString() {
        return "Segment{" + "path=" + path + ", count=" + count + '}';
    }
}
