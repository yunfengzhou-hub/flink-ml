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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.*;
import java.util.*;

@SuppressWarnings({"rawtypes", "cast"})
abstract class EmbedVertex implements Runnable {
    protected final EmbedOutput<StreamRecord> output;
    protected final List<StreamEdge> inEdges;
    protected final StreamOperatorFactory factory;
    protected final int id;

    public static EmbedVertex createEmbedGraphVertex(StreamNode node){
        return createEmbedGraphVertex(node.getOperatorFactory(), node.getInEdges(), node.getId());
    }

    public static EmbedVertex createEmbedGraphVertex(StreamOperatorFactory factory, List<StreamEdge> inEdges, int id){
        EmbedOutput<StreamRecord> output = new EmbedOutput<>(new ArrayList<>());
        StreamOperator<?> operator = StreamFunctionUtils.getStreamOperator(factory, output);
        if(operator instanceof OneInputStreamOperator){
            return new OneInputEmbedVertex(inEdges, id, output, (OneInputStreamOperator<?, ?>) operator, factory);
        }else if(operator instanceof TwoInputStreamOperator){
            return new TwoInputEmbedVertex(inEdges, id, output, (TwoInputStreamOperator<?, ?, ?>) operator, factory);
        }else if(operator instanceof StreamSource){
            return new SourceEmbedVertex(inEdges, id, output, factory);
        }else{
            throw new IllegalArgumentException(String.format("%s is not supported for %s yet.",
                    operator.getClass(), EmbedVertex.class));
        }
    }

    protected EmbedVertex(List<StreamEdge> inEdges, int id, EmbedOutput<StreamRecord> output, StreamOperatorFactory factory) {
        this.output = output;
        this.inEdges = inEdges;
        this.id = id;
        this.factory = factory;
    }

    public String serialize() {
        try {
            Map<String, Object> map = new HashMap<>();
            map.put("id", id);

            List<String> inEdgeStrings = new ArrayList<>();
            for(StreamEdge inEdge:inEdges){
                inEdgeStrings.add(toString(inEdge));
            }
            map.put("inEdges", inEdgeStrings);

            map.put("factory", toString(factory));

            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize vertex", e);
        }
    }

    public static EmbedVertex deserialize(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(json, Map.class);

            int id = (Integer)map.get("id");

            List<String> inEdgeStrings = (List<String>) map.get("inEdges");
            List<StreamEdge> inEdges = new ArrayList<>();
            for(String inEdgeString:inEdgeStrings){
                inEdges.add((StreamEdge)fromString(inEdgeString));
            }

            StreamOperatorFactory factory = (StreamOperatorFactory)fromString((String) map.get("factory"));
            return EmbedVertex.createEmbedGraphVertex(factory, inEdges, id);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize vertex json:" + json, e);
        }
    }


    private static Object fromString(String s) throws IOException, ClassNotFoundException {
        byte [] data = Base64.getDecoder().decode( s );
        ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(  data ) );
        Object o  = ois.readObject();
        ois.close();
        return o;
    }

    private static String toString(Serializable o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject( o );
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    public List<StreamEdge> getInEdges() {
        return inEdges;
    }

    public int getId() {
        return id;
    }

    public abstract List<StreamRecord> getInputList(int typeNumber);

    public abstract void clear();

    public List<StreamRecord> getOutput(){
        return output.getOutputList();
    }
}