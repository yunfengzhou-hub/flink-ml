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

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * A {@link SourceFunction} for unit tests. It collects records from a blocking queue managed by {@link TestBlockingQueueManager}.
 */
public class MockBlockingQueueSourceFunction<T> implements SourceFunction<T> {
    private final String id;
    private boolean isRunning = true;

    public MockBlockingQueueSourceFunction(String id) {
        this.id = id;
    }

    @Override
    public void run(SourceContext<T> context) throws Exception {
        while (isRunning) {
            context.collect(TestBlockingQueueManager.poll(id));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
