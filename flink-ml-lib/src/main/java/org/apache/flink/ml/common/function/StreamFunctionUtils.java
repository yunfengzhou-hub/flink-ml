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

package org.apache.flink.ml.common.function;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.common.function.environment.EmbedOperatorEventDispatcherImpl;
import org.apache.flink.ml.common.function.environment.EmbedProcessingTimeServiceImpl;
import org.apache.flink.ml.common.function.environment.EmbedRuntimeEnvironment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@SuppressWarnings({"unchecked", "rawtypes"})
class StreamFunctionUtils {
    public static StreamOperator getStreamOperator(StreamNode node, Output<StreamRecord> output){
        return getStreamOperator(node.getOperatorFactory(), output);
    }

    public static StreamOperator getStreamOperator(StreamOperatorFactory factory, Output<StreamRecord> output){
        EmbedRuntimeEnvironment env = new EmbedRuntimeEnvironment();
        StreamTask<?, ?> task;
        try {
            task = new OneInputStreamTask<>(env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        StreamConfig streamConfig = new StreamConfig(new Configuration());
        streamConfig.setOperatorID(new OperatorID());
        streamConfig.setOperatorName("operator name");

        Supplier<ProcessingTimeService> processingTimeServiceFactory = EmbedProcessingTimeServiceImpl::new;

        OperatorEventDispatcher operatorEventDispatcher =
                new EmbedOperatorEventDispatcherImpl(
                        env.getUserCodeClassLoader().asClassLoader(),
                        env.getOperatorCoordinatorEventGateway());

        StreamOperatorParameters parameters = new StreamOperatorParameters(
                task,
                streamConfig,
                output,
                processingTimeServiceFactory,
                operatorEventDispatcher
        );

        if(factory instanceof AbstractStreamOperatorFactory){
            ((AbstractStreamOperatorFactory)factory).setProcessingTimeService(processingTimeServiceFactory.get());
        }

        StreamOperator operator = factory.createStreamOperator(parameters);
        try {
            operator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException(String.format("Cannot initiate operator %s", operator));
        }
        return operator;
    }

    static void validateGraph(StreamGraph graph) {
        List<StreamNode> nodes = new ArrayList<>(graph.getStreamNodes());

        int sourceCount = 0;
        for(StreamNode node:nodes){
            StreamOperator operator = getStreamOperator(node, new EmbedOutput<>());

            if(operator instanceof StreamSource){
                sourceCount ++;
                if(sourceCount > 1){
                    throw new IllegalArgumentException(String.format("%s only allows one single stream source.", StreamFunction.class));
                }
                continue;
            }

            if(!(operator instanceof AbstractStreamOperator)){
                throw new IllegalArgumentException(String.format("%s only supports %s. %s is not supported yet.",
                        StreamFunction.class, AbstractStreamOperator.class, AbstractStreamOperatorV2.class));
            }

            if(operator instanceof AbstractUdfStreamOperator){
                if(((AbstractUdfStreamOperator<?, ?>) operator).getUserFunction() instanceof RichFunction){
                    throw new IllegalArgumentException("Stateful/Rich functions are not supported yet.");
                }
            }

        }
    }
}





