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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class StreamFunction<T, R> implements Function<T, List<R>> {
    private final DataStream<R> dataStream;

    public StreamFunction(DataStream<R> dataStream) {
        this.dataStream = dataStream;
        StreamFunctionUtils.validateGraph(dataStream);
    }

    /**
     * Applies the computation logic of stored StreamGraph to the input data. Output is collected and returned.
     *
     * @param t input data of the StreamGraph.
     * @return result after applying the computation logic to input data.
     */
    @Override
    public List<R> apply(T t){
        StreamGraph graph = StreamFunctionUtils.getStreamGraph(dataStream);
        List<StreamNode> nodes = new ArrayList<>(graph.getStreamNodes());
        StreamFunctionUtils.topologicalSort(nodes);

        Map<Integer, List<StreamRecord>> outputMap = new HashMap<>();
        List<StreamRecord> outputList = null;

        for(StreamNode node: nodes) {
            EmbedVertex vertex = EmbedVertex.createEmbedGraphVertex(node);
            outputList = vertex.getOutput().getOutputList();
            outputMap.put(vertex.getId(), outputList);

            if(vertex.getInEdges().size() == 0) {
                outputList.add(new StreamRecord(t));
                continue;
            }

            for(StreamEdge edge : vertex.getInEdges()){
                for(StreamRecord record:outputMap.get(edge.getSourceId())){
                    vertex.getInputList(edge.getTypeNumber()).add(new StreamRecord(record.getValue()));
                }
            }

            vertex.run();
        }

        List<R> result = new ArrayList<>();
        for(StreamRecord r:outputList) {
            result.add((R) r.getValue());
        }
        return result;
    }
}
