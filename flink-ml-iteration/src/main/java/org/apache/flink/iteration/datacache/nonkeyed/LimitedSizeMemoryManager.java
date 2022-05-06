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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A proxy class for {@link MemoryManager} that limits the size of memory allocated through this
 * object to a certain proportion.
 */
public class LimitedSizeMemoryManager {
    private final MemoryManager memoryManager;

    private final int maxAllowedPageNum;

    private int allocatedPageNum;

    private final Map<Object, Integer> allocatedPageMap = new HashMap<>();

    public LimitedSizeMemoryManager(MemoryManager memoryManager, double fraction) {
        this.memoryManager = memoryManager;
        this.maxAllowedPageNum = memoryManager.computeNumberOfPages(fraction);
    }

    public int getPageSize() {
        return memoryManager.getPageSize();
    }

    public long availableMemory() {
        return (long) (maxAllowedPageNum - allocatedPageNum) * memoryManager.getPageSize();
    }

    public long getMemorySize() {
        return (long) maxAllowedPageNum * memoryManager.getPageSize();
    }

    public List<MemorySegment> allocatePages(Object owner, int numPages)
            throws MemoryAllocationException {
        if (allocatedPageNum + numPages > maxAllowedPageNum) {
            throw new MemoryAllocationException(
                    String.format("Could not allocate %d pages", numPages));
        }

        List<MemorySegment> segments = memoryManager.allocatePages(owner, numPages);
        allocatedPageNum += numPages;
        allocatedPageMap.put(owner, allocatedPageMap.getOrDefault(owner, 0) + numPages);
        return segments;
    }

    public void releaseAll(Object owner) {
        int numPages = allocatedPageMap.getOrDefault(owner, 0);
        memoryManager.releaseAll(owner);
        allocatedPageMap.remove(owner);
        allocatedPageNum -= numPages;
    }
}
