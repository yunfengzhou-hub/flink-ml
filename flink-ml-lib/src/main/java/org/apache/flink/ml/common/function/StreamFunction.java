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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.planner.utils.ExecutorUtils;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

/**
 * A functional representation of the stream graph of a flink job. The
 * computation logic of the flink job can be invoked through this function's
 * {@link #apply(Object)} method.
 *
 * @param <T> class of the input data
 * @param <R> class of the output data
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class StreamFunction<T, R> implements Function<T, List<R>>, Serializable {
    private final Map<Integer, StreamOperatorFactory> factoryMap;
    private final Map<Integer, List<StreamEdge>> inEdgeMap;
    private final int outVertexId;
    private transient Map<Integer, List<StreamRecord>> intermediateResults;
    private transient Map<Integer, EmbedOutput> outputMap;
    private transient Map<Integer, StreamOperator> operatorMap;
    private transient boolean setup = false;

    public StreamFunction(DataStream<R> outStream) {
        outVertexId = outStream.getId();

        StreamGraph graph = ExecutorUtils.generateStreamGraph(
                outStream.getExecutionEnvironment(),
                Collections.singletonList(outStream.getTransformation()));

        factoryMap = new HashMap<>();
        inEdgeMap = new HashMap<>();
        for(StreamNode node:graph.getStreamNodes()){
            factoryMap.put(node.getId(), node.getOperatorFactory());
            inEdgeMap.put(node.getId(), node.getInEdges());
        }

        setup();
    }

    public void setup(){
        if(setup)   return;

        StreamFunctionUtils.validateGraph(factoryMap, inEdgeMap);

        outputMap = new HashMap<>();
        operatorMap = new HashMap<>();

        for(int nodeId:factoryMap.keySet()){
            StreamOperatorFactory factory = factoryMap.get(nodeId);
            EmbedOutput output = new EmbedOutput();
            StreamOperator operator = StreamFunctionUtils.getStreamOperator(factory, output);
            outputMap.put(nodeId, output);
            operatorMap.put(nodeId, operator);
        }

        setup = true;
    }

    @Override
    public List<R> apply(T t) {
        try{
            setup();
            intermediateResults = new HashMap<>();
            List<R> result = new ArrayList<>();
            for(StreamRecord r:execute(outVertexId, t)) {
                result.add((R) r.getValue());
            }
            return result;
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("Failed to apply function logic on input "+t);
        }
    }

    private List<StreamRecord> execute(int nodeId, T t) throws Exception {
        if(intermediateResults.containsKey(nodeId)){
            return intermediateResults.get(nodeId);
        }

        List<StreamRecord> result = new ArrayList<>();

        StreamOperator operator = operatorMap.get(nodeId);

        Map<Integer, List<StreamRecord>> inputMap = new HashMap<>();

        if(operator instanceof StreamSource){
            result.add(new StreamRecord(t));
        }else{
            outputMap.get(nodeId).getOutputList().clear();

            for(StreamEdge edge:inEdgeMap.get(nodeId)){
                for(StreamRecord record:execute(edge.getSourceId(), t)){
                    // some stateful operations require deep copy of StreamRecord values,
                    // but in stateless cases this could be avoided, bringing better performance.
                    inputMap.computeIfAbsent(edge.getTypeNumber(), k -> new ArrayList<>());
                    inputMap.get(edge.getTypeNumber()).add(new StreamRecord(record.getValue()));
                }
            }

            operator.close();
            operator.open();

            if(operator instanceof OneInputStreamOperator){
                OneInputStreamOperator oneInputStreamOperator = (OneInputStreamOperator)operator;
                for(StreamRecord record:inputMap.getOrDefault(0, new ArrayList<>())){
                    oneInputStreamOperator.processElement(record);
                }
            }else if(operator instanceof TwoInputStreamOperator){
                TwoInputStreamOperator twoInputStreamOperator = (TwoInputStreamOperator)operator;
                for(StreamRecord record:inputMap.getOrDefault(1, new ArrayList<>())){
                    twoInputStreamOperator.processElement1(record);
                }
                for(StreamRecord record:inputMap.getOrDefault(2, new ArrayList<>())){
                    twoInputStreamOperator.processElement2(record);
                }
            }else{
                throw new RuntimeException("Unsupported operator " + operator + "used.");
            }

            result = outputMap.get(nodeId).getOutputList();
        }

        intermediateResults.put(nodeId, result);

        return result;
    }
}
