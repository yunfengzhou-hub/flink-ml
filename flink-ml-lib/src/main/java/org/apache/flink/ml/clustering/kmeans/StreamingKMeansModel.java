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

package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.*;

/**
 * StreamingKMeansModel can be regarded as an advanced {@link KMeansModel} operator which can update
 * model data in a streaming format, using the model data provided by {@link StreamingKMeans}.
 */
public class StreamingKMeansModel
        implements Model<StreamingKMeansModel>, KMeansModelParams<StreamingKMeansModel> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private static final OutputTag<KMeansModelData> outputTag =
            new OutputTag<KMeansModelData>("latest-model-data") {};
    private Table modelDataTable;
    private DataStream<KMeansModelData> latestModelData;

    public StreamingKMeansModel() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public StreamingKMeansModel setModelData(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        modelDataTable = inputs[0];
        return this;
    }

    @Override
    public Table[] getModelData() {
        return new Table[] {modelDataTable};
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();

        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());
        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(inputTypeInfo.getFieldTypes(), Types.INT),
                        ArrayUtils.addAll(inputTypeInfo.getFieldNames(), getPredictionCol()));

        DataStream<Row> predictionResult =
                KMeansModelData.getModelDataStream(modelDataTable)
                        .broadcast()
                        .connect(tEnv.toDataStream(inputs[0]))
                        .process(
                                new PredictLabelFunction(
                                        getFeaturesCol(),
                                        DistanceMeasure.getInstance(getDistanceMeasure())),
                                outputTypeInfo);

        latestModelData = DataStreamUtils.getSideOutput(predictionResult, outputTag);

        return new Table[] {tEnv.fromDataStream(predictionResult)};
    }

    /**
     * Gets the data stream containing the latest model data used in this Model. The "latest" means
     * that If a model data is observed in this stream, the model data would have come into effect
     * in this model. Thus predict data arrived afterwards will be served by this model data, or a
     * later model data. It won't be served by any earlier model data.
     */
    @VisibleForTesting
    public DataStream<KMeansModelData> getLatestModelData() {
        return latestModelData;
    }

    /** A utility function used for prediction. */
    private static class PredictLabelFunction extends CoProcessFunction<KMeansModelData, Row, Row> {
        private final String featuresCol;

        private final DistanceMeasure distanceMeasure;

        private DenseVector[] centroids;

        private final List<Row> cache = new ArrayList<>();

        public PredictLabelFunction(String featuresCol, DistanceMeasure distanceMeasure) {
            this.featuresCol = featuresCol;
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void processElement1(
                KMeansModelData modelData,
                CoProcessFunction<KMeansModelData, Row, Row>.Context ctx,
                Collector<Row> collector) {
            centroids = modelData.centroids;
            ctx.output(outputTag, modelData);
            for (Row dataPoint : cache) {
                processElement2(dataPoint, ctx, collector);
            }
            cache.clear();
        }

        @Override
        public void processElement2(
                Row dataPoint,
                CoProcessFunction<KMeansModelData, Row, Row>.Context ctx,
                Collector<Row> collector) {
            if (centroids == null) {
                cache.add(dataPoint);
                return;
            }
            DenseVector point = (DenseVector) dataPoint.getField(featuresCol);
            int closestCentroidId = KMeans.findClosestCentroidId(centroids, point, distanceMeasure);
            collector.collect(Row.join(dataPoint, Row.of(closestCentroidId)));
        }
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    // TODO: Add INFO level logging.
    public static StreamingKMeansModel load(StreamExecutionEnvironment env, String path)
            throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }
}
