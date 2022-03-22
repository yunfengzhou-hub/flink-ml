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

package org.apache.flink.ml.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** A {@link SourceFunction} implementation that can directly receive records from tests. */
@SuppressWarnings({"unchecked", "rawtypes"})
public class InMemorySourceFunction<T> extends RichSourceFunction<T> {
    private static final Map<UUID, BlockingQueue> queueMap = new ConcurrentHashMap<>();
    private final UUID id;
    private BlockingQueue<T> queue;
    private boolean isRunning = true;

    public InMemorySourceFunction() {
        id = UUID.randomUUID();
        queue = new LinkedBlockingQueue();
        queueMap.put(id, queue);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        queue = queueMap.get(id);
    }

    @Override
    public void close() throws Exception {
        super.close();
        queueMap.remove(id);
    }

    @Override
    public void run(SourceContext<T> context) {
        while (isRunning) {
            T value = queue.poll();
            if (value == null) {
                Thread.yield();
            } else {
                context.collect(value);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @SafeVarargs
    public final void offerAll(T... values) throws InterruptedException {
        for (T value : values) {
            offer(value);
        }
    }

    public void offer(T value) throws InterruptedException {
        offer(value, 1, TimeUnit.MINUTES);
    }

    public void offer(T value, long timeout, TimeUnit unit) throws InterruptedException {
        if (!queue.offer(value, timeout, unit)) {
            throw new RuntimeException(
                    "Failed to offer " + value + " to blocking queue " + id + ".");
        }
    }
}
