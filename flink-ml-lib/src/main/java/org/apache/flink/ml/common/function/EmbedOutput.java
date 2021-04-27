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

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

class EmbedOutput<OUT> implements Output<OUT> {
    private final List<OUT> list;

    public EmbedOutput(){
        this.list = new ArrayList<>();
    }

    public EmbedOutput(List<OUT> list){
        this.list = list;
    }

    @Override
    public void emitWatermark(Watermark mark) {
        throw new UnsupportedOperationException(String.valueOf(this.getClass()));
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        throw new UnsupportedOperationException(String.valueOf(this.getClass()));
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        throw new UnsupportedOperationException(String.valueOf(this.getClass()));
    }

    @Override
    public void collect(OUT out) {
        list.add(out);
    }

    public List<OUT> getOutputList(){
        return list;
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException(String.valueOf(this.getClass()));
    }
}