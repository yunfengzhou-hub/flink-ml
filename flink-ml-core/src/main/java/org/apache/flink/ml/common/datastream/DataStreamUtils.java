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
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheReader;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheSnapshot;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheWriter;
import org.apache.flink.iteration.datacache.nonkeyed.Segment;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.IteratorUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/** Provides utility functions for {@link DataStream}. */
@Internal
public class DataStreamUtils {
    /**
     * Applies allReduceSum on the input data stream. The input data stream is supposed to contain
     * up to one double array in each partition. The result data stream has the same parallelism as
     * the input, where each partition contains one double array that sums all of the double arrays
     * in the input data stream.
     *
     * <p>Note that we throw exception when one of the following two cases happen:
     * <li>There exists one partition that contains more than one double array.
     * <li>The length of the double array is not consistent among all partitions.
     *
     * @param input The input data stream.
     * @return The result data stream.
     */
    public static DataStream<double[]> allReduceSum(DataStream<double[]> input) {
        return AllReduceImpl.allReduceSum(input);
    }

    /**
     * Applies a {@link MapPartitionFunction} on a bounded data stream.
     *
     * @param input The input data stream.
     * @param func The user defined mapPartition function.
     * @param <IN> The class type of the input.
     * @param <OUT> The class type of output.
     * @return The result data stream.
     */
    public static <IN, OUT> DataStream<OUT> mapPartition(
            DataStream<IN> input, MapPartitionFunction<IN, OUT> func) {
        TypeInformation<OUT> resultType =
                TypeExtractor.getMapPartitionReturnTypes(func, input.getType(), null, true);
        return input.transform(
                        "mapPartition",
                        resultType,
                        new MapPartitionOperator<>(func, input.getType()))
                .setParallelism(input.getParallelism());
    }

    /**
     * Applies a {@link ReduceFunction} on a bounded data stream. The output stream contains at most
     * one stream record and its parallelism is one.
     *
     * @param input The input data stream.
     * @param func The user defined reduce function.
     * @param <T> The class type of the input.
     * @return The result data stream.
     */
    public static <T> DataStream<T> reduce(DataStream<T> input, ReduceFunction<T> func) {
        DataStream<T> partialReducedStream =
                input.transform("reduce", input.getType(), new ReduceOperator<>(func))
                        .setParallelism(input.getParallelism());
        if (partialReducedStream.getParallelism() == 1) {
            return partialReducedStream;
        } else {
            return partialReducedStream
                    .transform("reduce", input.getType(), new ReduceOperator<>(func))
                    .setParallelism(1);
        }
    }

    /**
     * Takes a randomly sampled subset of elements in a bounded data stream.
     *
     * <p>If the number of elements in the stream is smaller than expected number of samples, all
     * elements will be included in the sample.
     *
     * @param input The input data stream.
     * @param numSamples The number of elements to be sampled.
     * @param randomSeed The seed to randomly pick elements as sample.
     * @return A data stream containing a list of the sampled elements.
     */
    public static <T> DataStream<List<T>> sample(
            DataStream<T> input, int numSamples, long randomSeed) {
        return input.transform(
                        "samplingOperator",
                        Types.LIST(input.getType()),
                        new SamplingOperator<>(numSamples, randomSeed))
                .setParallelism(1);
    }

    /**
     * A stream operator to apply {@link MapPartitionFunction} on each partition of the input
     * bounded data stream.
     */
    private static class MapPartitionOperator<IN, OUT>
            extends AbstractUdfStreamOperator<OUT, MapPartitionFunction<IN, OUT>>
            implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

        private final TypeInformation<IN> inputType;

        private Path basePath;

        private StreamConfig config;

        private StreamTask<?, ?> containingTask;

        private DataCacheWriter<IN> dataCacheWriter;

        public MapPartitionOperator(
                MapPartitionFunction<IN, OUT> mapPartitionFunc, TypeInformation<IN> inputType) {
            super(mapPartitionFunc);
            this.inputType = inputType;
        }

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<OUT>> output) {
            super.setup(containingTask, config, output);

            basePath =
                    OperatorUtils.getDataCachePath(
                            containingTask.getEnvironment().getTaskManagerInfo().getConfiguration(),
                            containingTask
                                    .getEnvironment()
                                    .getIOManager()
                                    .getSpillingDirectoriesPaths());

            this.config = config;
            this.containingTask = containingTask;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            List<StatePartitionStreamProvider> inputs =
                    IteratorUtils.toList(context.getRawOperatorStateInputs().iterator());
            Preconditions.checkState(
                    inputs.size() < 2, "The input from raw operator state should be one or zero.");

            List<Segment> priorFinishedSegments = new ArrayList<>();
            if (inputs.size() > 0) {

                InputStream inputStream = inputs.get(0).getStream();

                DataCacheSnapshot dataCacheSnapshot =
                        DataCacheSnapshot.recover(
                                inputStream,
                                basePath.getFileSystem(),
                                OperatorUtils.createDataCacheFileGenerator(
                                        basePath, "cache", config.getOperatorID()));

                priorFinishedSegments = dataCacheSnapshot.getSegments();
            }

            dataCacheWriter =
                    new DataCacheWriter<>(
                            inputType.createSerializer(containingTask.getExecutionConfig()),
                            basePath.getFileSystem(),
                            OperatorUtils.createDataCacheFileGenerator(
                                    basePath, "cache", config.getOperatorID()),
                            null,
                            priorFinishedSegments);
        }

        @Override
        public void processElement(StreamRecord<IN> input) throws Exception {
            dataCacheWriter.addRecord(input.getValue());
        }

        @Override
        public void endInput() throws Exception {
            dataCacheWriter.finishCurrentSegment();
            List<Segment> pendingSegments = dataCacheWriter.getFinishSegments();
            userFunction.mapPartition(
                    new DataCacheReaderIterable<>(containingTask, pendingSegments, inputType),
                    new TimestampedCollector<>(output));
            dataCacheWriter.cleanup();
        }

        private static class DataCacheReaderIterable<T> implements Iterable<T> {
            private final StreamTask<?, ?> containingTask;
            private final List<Segment> pendingSegments;
            private final TypeInformation<T> type;

            private DataCacheReaderIterable(
                    StreamTask<?, ?> containingTask,
                    List<Segment> pendingSegments,
                    TypeInformation<T> type) {
                this.containingTask = containingTask;
                this.pendingSegments = pendingSegments;
                this.type = type;
            }

            @Override
            public Iterator<T> iterator() {
                return new DataCacheReader<>(
                        type.createSerializer(containingTask.getExecutionConfig()),
                        null,
                        pendingSegments);
            }
        }
    }

    /** A stream operator to apply {@link ReduceFunction} on the input bounded data stream. */
    private static class ReduceOperator<T> extends AbstractUdfStreamOperator<T, ReduceFunction<T>>
            implements OneInputStreamOperator<T, T>, BoundedOneInput {
        /** The temp result of the reduce function. */
        private T result;

        private ListState<T> state;

        public ReduceOperator(ReduceFunction<T> userFunction) {
            super(userFunction);
        }

        @Override
        public void endInput() {
            if (result != null) {
                output.collect(new StreamRecord<>(result));
            }
        }

        @Override
        public void processElement(StreamRecord<T> streamRecord) throws Exception {
            if (result == null) {
                result = streamRecord.getValue();
            } else {
                result = userFunction.reduce(streamRecord.getValue(), result);
            }
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            state =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "state",
                                            getOperatorConfig()
                                                    .getTypeSerializerIn(
                                                            0, getClass().getClassLoader())));
            result = OperatorStateUtils.getUniqueElement(state, "state").orElse(null);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            state.clear();
            if (result != null) {
                state.add(result);
            }
        }
    }

    /**
     * A stream operator that takes a randomly sampled subset of elements in a bounded data stream.
     */
    private static class SamplingOperator<T> extends AbstractStreamOperator<List<T>>
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
                            getOperatorConfig()
                                    .getTypeSerializerIn(0, getClass().getClassLoader()));
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
}
