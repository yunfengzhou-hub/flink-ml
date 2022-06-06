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
import org.apache.flink.core.memory.MemorySegment;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A segment contains the information about a cache unit. */
@Internal
public class Segment {
    private final Path path;

    private final int count;

    private long fsSize = -1L;

    private List<MemorySegment> cache;

    Segment(Path path, int count, long fsSize) {
        this.path = checkNotNull(path);
        checkArgument(count > 0);
        this.count = count;
        checkArgument(fsSize > 0);
        this.fsSize = fsSize;
    }

    Segment(Path path, int count, List<MemorySegment> cache) {
        this.path = checkNotNull(path);
        checkArgument(count > 0);
        this.count = count;
        this.cache = checkNotNull(cache);
    }

    void setCache(List<MemorySegment> cache) {
        this.cache = checkNotNull(cache);
    }

    void setDiskInfo(long fsSize) {
        checkArgument(fsSize > 0);
        this.fsSize = fsSize;
    }

    boolean isOnDisk() {
        return fsSize > 0;
    }

    boolean isCached() {
        return cache != null;
    }

    Path getPath() {
        return path;
    }

    int getCount() {
        return count;
    }

    long getFsSize() {
        checkState(fsSize > 0);
        return fsSize;
    }

    List<MemorySegment> getCache() {
        return checkNotNull(cache);
    }
}
