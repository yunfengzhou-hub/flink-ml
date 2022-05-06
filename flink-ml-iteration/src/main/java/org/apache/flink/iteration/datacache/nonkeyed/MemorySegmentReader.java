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

import java.util.List;

/** A class that reads data cached in memory. */
@Internal
public class MemorySegmentReader<T> implements SegmentReader<T> {
    private final Segment segment;

    private final List<T> cache;

    private int globalCount;

    public MemorySegmentReader(Segment segment, int startOffset) {
        this.segment = segment;
        this.cache = segment.getCache();
        this.globalCount = startOffset;
    }

    @Override
    public boolean hasNext() {
        return globalCount < segment.getCount();
    }

    @Override
    public T next() {
        return cache.get(globalCount++);
    }

    @Override
    public void close() {}

    @Override
    public int getOffset() {
        return globalCount;
    }
}
