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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheReader;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheSnapshot;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheWriter;
import org.apache.flink.iteration.datacache.nonkeyed.Segment;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.streaming.api.graph.StreamConfig;
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
import java.util.Iterator;
import java.util.List;

/**
 * A stream operator to apply {@link MapPartitionFunction} on each partition of the input bounded
 * data stream.
 */
@Internal
class MapPartitionOperator<IN, OUT>
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
                        basePath.getFileSystem(),
                        OperatorUtils.createDataCacheFileGenerator(
                                basePath, "cache", config.getOperatorID()),
                        null,
                        inputType.createSerializer(containingTask.getExecutionConfig()),
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
                    null,
                    type.createSerializer(containingTask.getExecutionConfig()),
                    pendingSegments);
        }
    }
}
