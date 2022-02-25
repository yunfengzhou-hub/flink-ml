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
import org.apache.flink.ml.clustering.kmeans.KMeansModelData.ModelDataDecoder;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** A Model which clusters data into k clusters using the model data computed by {@link KMeans}. */
public class KMeansModel implements Model<KMeansModel>, KMeansModelParams<KMeansModel> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private final OutputTag<DenseVector[]> outputTag = new OutputTag<DenseVector[]>("latest-model-data"){};
    private Table modelDataTable;
    private DataStream<DenseVector[]> latestModelData;

    public KMeansModel() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public KMeansModel setModelData(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        modelDataTable = inputs[0];
        return this;
    }

    @Override
    public Table[] getModelData() {
        return new Table[] {modelDataTable};
    }

    @Override
    @SuppressWarnings("unchecked")
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<KMeansModelData> modelDataStream =
                KMeansModelData.getModelDataStream(modelDataTable);

        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());
        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(inputTypeInfo.getFieldTypes(), Types.INT),
                        ArrayUtils.addAll(inputTypeInfo.getFieldNames(), getPredictionCol()));

        final String broadcastModelKey = "broadcastModelKey";

        SingleOutputStreamOperator<Row> predictionResult =
                modelDataStream
                        .broadcast()
                        .connect(tEnv.toDataStream(inputs[0]))
                        .process(new PredictLabelFunction2(
                                        broadcastModelKey,
                                        getFeaturesCol(),
                                        DistanceMeasure.getInstance(getDistanceMeasure())),
                                outputTypeInfo);

        latestModelData = predictionResult.getSideOutput(outputTag);

        return new Table[] {tEnv.fromDataStream(predictionResult)};
    }

    @VisibleForTesting
    public DataStream<DenseVector[]> getLatestModelData() {
        return latestModelData;
    }

    private static class PredictLabelFunction2 extends CoProcessFunction<KMeansModelData, Row, Row> {
        private final OutputTag<DenseVector[]> outputTag = new OutputTag<DenseVector[]>("latest-model-data"){};

        private final String broadcastModelKey;

        private final String featuresCol;

        private final DistanceMeasure distanceMeasure;

        private DenseVector[] centroids = new DenseVector[0];

        public PredictLabelFunction2(
                String broadcastModelKey, String featuresCol, DistanceMeasure distanceMeasure) {
            this.broadcastModelKey = broadcastModelKey;
            this.featuresCol = featuresCol;
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void processElement1(KMeansModelData modelData, CoProcessFunction<KMeansModelData, Row, Row>.Context ctx, Collector<Row> collector) throws Exception {

            centroids = modelData.centroids;
            System.out.println("KMeansModel received model data " + Arrays.toString(centroids));
            ctx.output(outputTag, centroids);
        }

        @Override
        public void processElement2(Row dataPoint, CoProcessFunction<KMeansModelData, Row, Row>.Context ctx, Collector<Row> collector) throws Exception {

            DenseVector point = (DenseVector) dataPoint.getField(featuresCol);
            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;
            for (int i = 0; i < centroids.length; i++) {
                DenseVector centroid = centroids[i];
                double distance = distanceMeasure.distance(centroid, point);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = i;
                }
            }
            collector.collect(Row.join(dataPoint, Row.of(closestCentroidId)));
        }
    }

    /** A utility function used for prediction. */
    private static class PredictLabelFunction
            implements CoFlatMapFunction<KMeansModelData, Row, Row> {

        private final String broadcastModelKey;

        private final String featuresCol;

        private final DistanceMeasure distanceMeasure;

        private DenseVector[] centroids = new DenseVector[0];

        public PredictLabelFunction(
                String broadcastModelKey, String featuresCol, DistanceMeasure distanceMeasure) {
            this.broadcastModelKey = broadcastModelKey;
            this.featuresCol = featuresCol;
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void flatMap1(KMeansModelData modelData, Collector<Row> collector) throws Exception {
            centroids = modelData.centroids;
            System.out.println("KMeansModel received model data " + Arrays.toString(centroids));

        }

        @Override
        public void flatMap2(Row dataPoint, Collector<Row> collector) throws Exception {
            DenseVector point = (DenseVector) dataPoint.getField(featuresCol);
            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;
            for (int i = 0; i < centroids.length; i++) {
                DenseVector centroid = centroids[i];
                double distance = distanceMeasure.distance(centroid, point);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = i;
                }
            }
            collector.collect(Row.join(dataPoint, Row.of(closestCentroidId)));
        }
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveModelData(
                KMeansModelData.getModelDataStream(modelDataTable),
                path,
                new KMeansModelData.ModelDataEncoder());
        ReadWriteUtils.saveMetadata(this, path);
    }

    // TODO: Add INFO level logging.
    public static KMeansModel load(StreamExecutionEnvironment env, String path) throws IOException {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<KMeansModelData> modelData =
                ReadWriteUtils.loadModelData(env, path, new ModelDataDecoder());
        KMeansModel model = ReadWriteUtils.loadStageParam(path);
        return model.setModelData(tEnv.fromDataStream(modelData));
    }
}
