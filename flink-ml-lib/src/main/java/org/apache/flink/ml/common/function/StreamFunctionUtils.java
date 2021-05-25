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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.common.function.environment.EmbedOperatorEventDispatcherImpl;
import org.apache.flink.ml.common.function.environment.EmbedProcessingTimeServiceImpl;
import org.apache.flink.ml.common.function.environment.EmbedRuntimeEnvironment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.*;
import java.util.function.Supplier;

@SuppressWarnings({"unchecked", "rawtypes"})
class StreamFunctionUtils {
    private static final Set<Class<?>> allowedPartitionerClass = new HashSet<>(
            Arrays.asList(ForwardPartitioner.class, RebalancePartitioner.class));

    public static StreamOperator getStreamOperator(StreamOperatorFactory factory, Output<StreamRecord> output) throws Exception{
        EmbedRuntimeEnvironment env = new EmbedRuntimeEnvironment();
        StreamTask<?, ?> task = new OneInputStreamTask<>(env);
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
        operator.open();
        return operator;
    }

    static void validateGraph(Map<Integer, StreamOperatorFactory> factoryMap, Map<Integer, List<StreamEdge>> inEdgeMap) throws Exception{
        int sourceCount = 0;

        for(int nodeId:factoryMap.keySet()){
            List<StreamEdge> inEdges = inEdgeMap.get(nodeId);
            for(StreamEdge edge:inEdges){
                if(!allowedPartitionerClass.contains(edge.getPartitioner().getClass())){
                    throw new IllegalArgumentException(
                            "Only FORWARD and REBALANCE partition mode is supported");
                }
            }

            StreamOperatorFactory factory = factoryMap.get(nodeId);
            StreamOperator operator = getStreamOperator(factory, new EmbedOutput<>());

            if(!((operator instanceof StreamSource)
            || (operator instanceof OneInputStreamOperator)
            || (operator instanceof TwoInputStreamOperator))){
                throw new IllegalArgumentException(
                        String.format(
                                "Stream Operator class %s is not supported.", operator.getClass()
                        )
                );
            }

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
                Function function = ((AbstractUdfStreamOperator<?, ?>) operator).getUserFunction();
                if(function instanceof RichFunction
                || function instanceof CheckpointedFunction
                || function instanceof ListCheckpointed){
                    throw new IllegalArgumentException("Stateful/Rich functions are not supported yet.");
                }
            }
        }
    }
}





