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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.planner.utils.ExecutorUtils;

import java.util.*;

@Internal
@SuppressWarnings({"unchecked", "rawtypes"})
public class EmbedStreamFunction<T, R> implements StreamFunction<T, R> {

    private Map<Integer, List<StreamRecord>> outputMap;
    private final Map<Integer, EmbedVertex> vertexMap = new HashMap<>();
    private final int resultOperatorId;

    public EmbedStreamFunction(DataStream<R> stream) {
        this(ExecutorUtils.generateStreamGraph(
                StreamExecutionEnvironment.createLocalEnvironment().setStateBackend(new MemoryStateBackend()),
                Collections.singletonList(stream.getTransformation()))
        );
    }

    public EmbedStreamFunction(StreamGraph graph) {
        StreamFunctionUtils.validateGraph(graph);

        List<StreamNode> nodes = new ArrayList<>(graph.getStreamNodes());
        nodes.sort(Comparator.comparingInt(StreamNode::getId));
        this.resultOperatorId = nodes.get(nodes.size() - 1).getId();

        for(StreamNode node: nodes) {
            EmbedVertex vertex = EmbedVertex.createEmbedGraphVertex(node, graph.getStateBackend());

//            try {
//                vertex.getOperator().initializeState(new StreamTaskStateInitializerImpl(new EmbedRuntimeEnvironment(), graph.getStateBackend()));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }

            vertexMap.put(vertex.getId(), vertex);
        }
    }

    /**
     * Applies the computation logic of stored StreamGraph to the input data. Output is collected and returned.
     *
     * @param t input data of the StreamGraph.
     * @return result after applying the computation logic to input data.
     */
    @Override
    public List<R> apply(T t){
        outputMap = new HashMap<>();

        List<R> result = new ArrayList<>();
        for(StreamRecord r:execute(resultOperatorId, t)) {
            result.add((R) r.getValue());
        }
        return result;
    }

    private List<StreamRecord> execute(int vertexId, T t){
        List<StreamRecord> result = outputMap.get(vertexId);

        if(result != null){
            return result;
        }

        EmbedVertex vertex = vertexMap.get(vertexId);

        if(vertex instanceof SourceEmbedVertex) {
            result = new ArrayList<>();
            result.add(new StreamRecord(t));
        } else {
            vertex.clear();

            for(StreamEdge edge:vertex.getInEdges()){
                for(StreamRecord record:execute(edge.getSourceId(), t)){
                    vertex.getInputList(edge.getTypeNumber()).add(new StreamRecord(record.getValue()));
                }
            }

            vertex.run();

            result = vertex.getOutput();
        }

        outputMap.put(vertexId, result);

        return result;
    }
}
