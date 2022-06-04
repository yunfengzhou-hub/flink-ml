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
class Segment {

    private FileSegment fileSegment;

    private MemorySegment memorySegment;

    Segment(FileSegment fileSegment) {
        this.fileSegment = fileSegment;
    }

    Segment(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
    }

    void setFileSegment(FileSegment fileSegment) {
        this.fileSegment = fileSegment;
    }

    FileSegment getFileSegment() {
        return fileSegment;
    }

    void setMemorySegment(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
    }

    MemorySegment getMemorySegment() {
        return memorySegment;
    }
}
