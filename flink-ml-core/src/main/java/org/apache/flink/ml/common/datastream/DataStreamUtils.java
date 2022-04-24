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

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheReader;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheWriter;
import org.apache.flink.iteration.datacache.nonkeyed.Segment;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.List;

/** Provides utility functions for {@link DataStream}. */
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
     * @param <IN> The class type of the input element.
     * @param <OUT> The class type of output element.
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
     * A stream operator to apply {@link MapPartitionFunction} on each partition of the input
     * bounded data stream.
     */
    private static class MapPartitionOperator<IN, OUT>
            extends AbstractUdfStreamOperator<OUT, MapPartitionFunction<IN, OUT>>
            implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

        private Path basePath;
        private StreamConfig config;
        private StreamTask<?, ?> containingTask;
        private DataCacheWriter<IN> dataCacheWriter;
        private final TypeInformation<IN> inputType;

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
            dataCacheWriter =
                    new DataCacheWriter<>(
                            inputType.createSerializer(containingTask.getExecutionConfig()),
                            basePath.getFileSystem(),
                            OperatorUtils.createDataCacheFileGenerator(
                                    basePath, "cache", config.getOperatorID()));
        }

        @Override
        public void endInput() throws Exception {
            dataCacheWriter.finishCurrentSegment();
            List<Segment> pendingSegments = dataCacheWriter.getFinishSegments();
            DataCacheReader<IN> dataCacheReader =
                    new DataCacheReader<>(
                            inputType.createSerializer(containingTask.getExecutionConfig()),
                            basePath.getFileSystem(),
                            pendingSegments);
            userFunction.mapPartition(() -> dataCacheReader, new TimestampedCollector<>(output));
        }

        @Override
        public void processElement(StreamRecord<IN> input) throws Exception {
            dataCacheWriter.addRecord(input.getValue());
        }
    }
}
