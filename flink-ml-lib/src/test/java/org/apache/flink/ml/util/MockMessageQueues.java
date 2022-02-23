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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** A class that manages global message queues used in unit tests. */
@SuppressWarnings({"unchecked", "rawtypes"})
public class MockMessageQueues {
    private static final Map<String, BlockingQueue> queueMap = new HashMap<>();
    private static long counter = 0;

    public static synchronized String createMessageQueue() {
        String id = String.valueOf(counter);
        queueMap.put(id, new LinkedBlockingQueue<>());
        counter++;
        return id;
    }

    @SafeVarargs
    public static <T> void offerAll(String id, T... values) throws InterruptedException {
        for (T value : values) {
            offer(id, value);
        }
    }

    public static <T> void offer(String id, T value) throws InterruptedException {
        offer(id, value, 1, TimeUnit.MINUTES);
    }

    public static <T> void offer(String id, T value, long timeout, TimeUnit unit)
            throws InterruptedException {
        boolean success = queueMap.get(id).offer(value, timeout, unit);
        if (!success) {
            throw new RuntimeException(
                    "Failed to offer " + value + " to blocking queue " + id + ".");
        }
    }

    public static <T> List<T> poll(String id, int num) throws InterruptedException {
        List<T> result = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            result.add(poll(id));
        }
        return result;
    }

    public static <T> T poll(String id) throws InterruptedException {
        return poll(id, 1, TimeUnit.MINUTES);
    }

    public static <T> T poll(String id, long timeout, TimeUnit unit) throws InterruptedException {
        T value = (T) queueMap.get(id).poll(timeout, unit);
        if (value == null) {
            throw new RuntimeException("Failed to poll next value from blocking queue " + id + ".");
        }
        return value;
    }

    public static void deleteBlockingQueue(String id) {
        queueMap.remove(id);
    }
}
