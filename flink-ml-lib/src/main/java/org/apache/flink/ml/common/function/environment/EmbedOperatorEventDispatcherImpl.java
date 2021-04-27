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

package org.apache.flink.ml.common.function.environment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class EmbedOperatorEventDispatcherImpl implements OperatorEventDispatcher {

    private final Map<OperatorID, OperatorEventHandler> handlers;

    private final TaskOperatorEventGateway toCoordinator;

    public EmbedOperatorEventDispatcherImpl(ClassLoader classLoader, TaskOperatorEventGateway toCoordinator) {
        ClassLoader classLoader1 = checkNotNull(classLoader);
        this.toCoordinator = checkNotNull(toCoordinator);
        this.handlers = new HashMap<>();
    }

    @Override
    public void registerEventHandler(OperatorID operator, OperatorEventHandler handler) {
        throw new UnsupportedOperationException();
//        }
    }

    @Override
    public OperatorEventGateway getOperatorEventGateway(OperatorID operatorId) {
        throw new UnsupportedOperationException();
    }
}