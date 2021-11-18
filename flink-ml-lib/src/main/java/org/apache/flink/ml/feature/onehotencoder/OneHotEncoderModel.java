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

package org.apache.flink.ml.feature.onehotencoder;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

/**
 * A Model which encodes data into one-hot format using the model data computed by {@link
 * OneHotEncoder}.
 */
public class OneHotEncoderModel
        implements Model<OneHotEncoderModel>, OneHotEncoderModelParams<OneHotEncoderModel> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private Table modelTable;

    public OneHotEncoderModel() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        final String[] inputCols = getInputCols();
        final String[] outputCols = getOutputCols();
        final boolean dropLast = getDropLast();
        final String broadcastModelKey = "OneHotModelStream";

        Preconditions.checkArgument(inputCols.length == outputCols.length);

        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());
        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(
                                inputTypeInfo.getFieldTypes(),
                                Collections.nCopies(
                                                outputCols.length,
                                                ExternalTypeInfo.of(Vector.class))
                                        .toArray(new TypeInformation[0])),
                        ArrayUtils.addAll(inputTypeInfo.getFieldNames(), outputCols));

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) modelTable).getTableEnvironment();
        DataStream<Row> input = tEnv.toDataStream(inputs[0]);
        DataStream<Tuple2<Integer, Integer>> modelStream =
                OneHotEncoderModelData.toDataStream(tEnv, modelTable);

        Map<String, DataStream<?>> broadcastMap = new HashMap<>();
        broadcastMap.put(broadcastModelKey, modelStream);

        Function<List<DataStream<?>>, DataStream<Row>> function =
                dataStreams -> {
                    DataStream stream = dataStreams.get(0);
                    return stream.transform(
                            this.getClass().getSimpleName(),
                            outputTypeInfo,
                            new PredictLabelOperator(
                                    new PredictLabelFunction(
                                            inputCols, dropLast, broadcastModelKey)));
                };

        DataStream<Row> output =
                BroadcastUtils.withBroadcastStream(
                        Collections.singletonList(input), broadcastMap, function);

        Table outputTable = tEnv.fromDataStream(output);

        return new Table[] {outputTable};
    }

    @Override
    public void save(String path) throws IOException {
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) modelTable).getTableEnvironment();

        String dataPath = ReadWriteUtils.getDataPath(path);
        FileSink<Tuple2<Integer, Integer>> sink =
                FileSink.forRowFormat(
                                new Path(dataPath), new OneHotEncoderModelData.ModelDataEncoder())
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .withBucketAssigner(new BasePathBucketAssigner<>())
                        .build();
        OneHotEncoderModelData.toDataStream(tEnv, modelTable).sinkTo(sink);

        ReadWriteUtils.saveMetadata(this, path);
    }

    public static OneHotEncoderModel load(StreamExecutionEnvironment env, String path)
            throws IOException {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Source<Tuple2<Integer, Integer>, ?, ?> source =
                FileSource.forRecordStreamFormat(
                                new OneHotEncoderModelData.ModelDataStreamFormat(),
                                ReadWriteUtils.getDataPaths(path))
                        .build();
        OneHotEncoderModel model = ReadWriteUtils.loadStageParam(path);
        DataStream<Tuple2<Integer, Integer>> modelData =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "modelData");
        model.setModelData(OneHotEncoderModelData.fromDataStream(tEnv, modelData));

        return model;
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public OneHotEncoderModel setModelData(Table... inputs) {
        modelTable = inputs[0];
        return this;
    }

    @Override
    public Table[] getModelData() {
        return new Table[] {modelTable};
    }

    private static class PredictLabelOperator
            extends AbstractUdfStreamOperator<Row, PredictLabelFunction>
            implements OneInputStreamOperator<Row, Row> {

        public PredictLabelOperator(PredictLabelFunction userFunction) {
            super(userFunction);
        }

        @Override
        public void processElement(StreamRecord<Row> streamRecord) throws Exception {
            output.collect(new StreamRecord<>(userFunction.map(streamRecord.getValue())));
        }
    }

    private static class PredictLabelFunction extends RichMapFunction<Row, Row> {
        private final String[] inputCols;
        private final boolean dropLast;
        private final String broadcastModelKey;

        private PredictLabelFunction(
                String[] inputCols, boolean dropLast, String broadcastModelKey) {
            this.inputCols = inputCols;
            this.dropLast = dropLast;
            this.broadcastModelKey = broadcastModelKey;
        }

        @Override
        public Row map(Row row) {
            List<Tuple2<Integer, Integer>> model =
                    getRuntimeContext().getBroadcastVariable(broadcastModelKey);
            int[] categorySizes = new int[model.size()];
            int offset = dropLast ? 0 : 1;
            for (Tuple2<Integer, Integer> tup : model) {
                categorySizes[tup.f0] = tup.f1 + offset;
            }
            Row result = new Row(categorySizes.length);
            int idx;
            for (int i = 0; i < categorySizes.length; i++) {
                Number number = (Number) row.getField(inputCols[i]);
                Preconditions.checkArgument(number.intValue() == number.doubleValue());
                idx = number.intValue();
                if (idx == categorySizes[i]) {
                    result.setField(i, Vectors.sparse(categorySizes[i]));
                } else {
                    result.setField(i, Vectors.sparse(categorySizes[i], idx, 1.0));
                }
            }

            return Row.join(row, result);
        }
    }
}
