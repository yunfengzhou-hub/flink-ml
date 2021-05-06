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

import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

abstract class EmbedVertex implements Runnable {
    protected final EmbedOutput<StreamRecord> output;
    protected final List<StreamEdge> inEdges;
    protected final int id;

    public static EmbedVertex createEmbedGraphVertex(StreamNode node){
        EmbedOutput<StreamRecord> output = new EmbedOutput<>(new ArrayList<>());
        StreamOperator<?> operator = StreamFunctionUtils.getStreamOperator(node, output);
        if(operator instanceof OneInputStreamOperator){
            return new OneInputEmbedVertex(node, output, (OneInputStreamOperator<?, ?>) operator);
        }else if(operator instanceof TwoInputStreamOperator){
            return new TwoInputEmbedVertex(node, output, (TwoInputStreamOperator<?, ?, ?>) operator);
        }else if(operator instanceof StreamSource){
            return new SourceEmbedVertex(node, output);
        }else{
            throw new IllegalArgumentException(String.format("%s only supports %s, %s and %s. %s is not supported yet.",
                    EmbedVertex.class, StreamSource.class, OneInputStreamOperator.class, TwoInputStreamOperator.class, operator.getClass()));
        }
    }

    protected EmbedVertex(StreamNode node, EmbedOutput<StreamRecord> output) {
        this.output = output;
        this.inEdges = node.getInEdges();
        this.id = node.getId();
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