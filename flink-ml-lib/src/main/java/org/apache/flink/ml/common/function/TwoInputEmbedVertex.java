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
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"unchecked", "rawtypes"})
class TwoInputEmbedVertex extends EmbedVertex {
    private final TwoInputStreamOperator operator;
    protected final List<StreamRecord> input1 = new ArrayList<>();
    protected final List<StreamRecord> input2 = new ArrayList<>();

    public TwoInputEmbedVertex(
            List<StreamEdge> inEdges,
            int id,
            EmbedOutput<StreamRecord> output,
            TwoInputStreamOperator operator,
            StreamOperatorFactory factory){
        super(inEdges, id, output, factory);
        this.operator = operator;
    }

    public List<StreamRecord> getInputList(int typeNumber) {
        if(typeNumber == 1){
            return input1;
        }else if(typeNumber == 2){
            return input2;
        }else{
            throw new RuntimeException(String.format("Illegal typeNumber: %d", typeNumber));
        }
    }

    @Override
    public void clear() {
        input1.clear();
        input2.clear();
        output.getOutputList().clear();
    }

    @Override
    public void run() {
        try {
            operator.close();
            operator.open();
            for(StreamRecord record : input1){
                operator.processElement1(record);
            }
            for(StreamRecord record : input2){
                operator.processElement2(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(String.format("Operator %s failed", operator));
        }
    }
}
