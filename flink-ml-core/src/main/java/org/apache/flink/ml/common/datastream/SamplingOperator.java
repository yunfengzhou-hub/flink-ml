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

package org.apache.flink.ml.common.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/** A stream operator that takes a randomly sampled subset of elements in a bounded data stream. */
@Internal
class SamplingOperator<T> extends AbstractStreamOperator<List<T>>
        implements OneInputStreamOperator<T, List<T>>, BoundedOneInput {
    private final int numSamples;

    private final Random random;

    private ListState<T> samplesState;

    private List<T> samples;

    private ListState<Integer> countState;

    private int count;

    SamplingOperator(int numSamples, long randomSeed) {
        this.numSamples = numSamples;
        this.random = new Random(randomSeed);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        ListStateDescriptor<T> samplesDescriptor =
                new ListStateDescriptor<>(
                        "samplesState",
                        getOperatorConfig().getTypeSerializerIn(0, getClass().getClassLoader()));
        samplesState = context.getOperatorStateStore().getListState(samplesDescriptor);
        samples = new ArrayList<>();
        samplesState.get().forEach(samples::add);

        ListStateDescriptor<Integer> countDescriptor =
                new ListStateDescriptor<>("countState", IntSerializer.INSTANCE);
        countState = context.getOperatorStateStore().getListState(countDescriptor);
        Iterator<Integer> countIterator = countState.get().iterator();
        if (countIterator.hasNext()) {
            count = countIterator.next();
        } else {
            count = 0;
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        samplesState.update(samples);
        countState.update(Collections.singletonList(count));
    }

    @Override
    public void processElement(StreamRecord<T> streamRecord) throws Exception {
        T sample = streamRecord.getValue();
        count++;

        // Code below is inspired by the Reservoir Sampling algorithm.
        if (samples.size() < numSamples) {
            samples.add(sample);
        } else {
            if (random.nextInt(count) < numSamples) {
                samples.set(random.nextInt(numSamples), sample);
            }
        }
    }

    @Override
    public void endInput() throws Exception {
        Collections.shuffle(samples, random);
        output.collect(new StreamRecord<>(samples));
    }
}
