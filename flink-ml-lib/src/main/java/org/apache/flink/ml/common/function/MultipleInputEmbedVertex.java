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

import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings({"unchecked", "rawtypes"})
class MultipleInputEmbedVertex extends EmbedVertex {
    private final MultipleInputStreamOperator operator;
    protected final List<List<StreamRecord>> inputs = new ArrayList<>();

    public MultipleInputEmbedVertex(
            StreamNode node,
            EmbedOutput output,
            MultipleInputStreamOperator operator){
        super(node, output);
        this.operator = operator;
        for(int i=0;i<operator.getInputs().size();i++){
            inputs.add(new ArrayList<>());
        }
    }

    @Override
    public List<StreamRecord> getInputList(int typeNumber){
        return inputs.get(typeNumber-1);
    }

    @Override
    public StreamOperator getOperator() {
        return operator;
    }

    @Override
    public void clear() {
        for(List<StreamRecord> input:inputs){
            input.clear();
        }
        output.getOutputList().clear();
    }

    @Override
    public void run() {
        try {
            System.out.println(operator.getClass());
            System.out.println(Arrays.toString(operator.getClass().getInterfaces()));
//            operator.close();
//            operator.open();
            for(int i = 0; i < inputs.size(); i++){
                List<StreamRecord> input = inputs.get(i);
                Input inputOperator = (Input) operator.getInputs().get(i);
                for(StreamRecord record : input){
                    inputOperator.setKeyContextElement(record);
                    System.out.println("to process "+record.getValue());
                    inputOperator.processElement(record);
                }
                if(operator instanceof BoundedMultiInput){
                    System.out.println("endInput "+i);
                    ((BoundedMultiInput)operator).endInput(i+1);
                }
            }
//            if(operator instanceof BoundedMultiInput){
//                operator.close();
//                operator.open();
//            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}