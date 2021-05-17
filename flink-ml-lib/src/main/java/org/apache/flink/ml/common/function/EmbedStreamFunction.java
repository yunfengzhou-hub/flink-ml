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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
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
    private final Map<Integer, EmbedVertex> vertexMap;
    private final int outOperatorId;

    public EmbedStreamFunction(DataStream<R> outStream) {
        StreamGraph graph = ExecutorUtils.generateStreamGraph(
                outStream.getExecutionEnvironment(),
                Collections.singletonList(outStream.getTransformation()));
        StreamFunctionUtils.validateGraph(graph);
        List<StreamNode> nodes = new ArrayList<>(graph.getStreamNodes());
        this.outOperatorId = outStream.getId();

        vertexMap = new HashMap<>();
        for(StreamNode node: nodes) {
            EmbedVertex vertex = EmbedVertex.createEmbedGraphVertex(node);
            vertexMap.put(vertex.getId(), vertex);
        }
    }

    private EmbedStreamFunction(int outOperatorId, Map<Integer, EmbedVertex> vertexMap) {
        this.outOperatorId = outOperatorId;
        this.vertexMap = vertexMap;
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
        for(StreamRecord r:execute(outOperatorId, t)) {
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
                    // some stateful operations require deep copy of StreamRecord values,
                    // but in stateless cases this could be avoided, bringing better performance.
                    vertex.getInputList(edge.getTypeNumber()).add(new StreamRecord(record.getValue()));
                }
            }

            vertex.run();

            result = vertex.getOutput();
        }

        outputMap.put(vertexId, result);

        return result;
    }

    public String serialize() {
        try {
            ObjectMapper mapper = new ObjectMapper();

            Map<String, String> map = new HashMap<>();
            map.put("outOperatorId", Integer.toString(outOperatorId));

            Map<String, String> vertexJsonMap = new HashMap<>();
            for(Map.Entry<Integer, EmbedVertex> entry:vertexMap.entrySet()){
                vertexJsonMap.put(entry.getKey().toString(), entry.getValue().serialize());
            }
            map.put("vertexJsonMap", mapper.writeValueAsString(vertexJsonMap));
            return mapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize EmbedStreamFunction", e);
        }
    }

    public static <T, R> EmbedStreamFunction<T, R> deserialize(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, String> map = mapper.readValue(json, Map.class);

            int outOperatorId = Integer.parseInt(map.get("outOperatorId"));

            Map<String, String> vertexJsonMap = (Map<String, String>) mapper.readValue(map.get("vertexJsonMap"), Map.class);
            Map<Integer, EmbedVertex> vertexMap = new HashMap<>();
            for(Map.Entry<String, String> entry:vertexJsonMap.entrySet()){
                vertexMap.put(Integer.parseInt(entry.getKey()), EmbedVertex.deserialize(entry.getValue()));
            }

            return new EmbedStreamFunction<>(outOperatorId, vertexMap);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize EmbedStreamFunction json:" + json, e);
        }
    }
}
