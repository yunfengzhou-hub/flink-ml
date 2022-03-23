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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.IteratorUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * OnlineKMeans extends the function of {@link KMeans}, supporting to train a K-Means model
 * continuously according to an unbounded stream of train data.
 *
 * <p>OnlineKMeans makes updates with the "mini-batch" KMeans rule, generalized to incorporate
 * forgetfulness (i.e. decay). After the centroids estimated on the current batch are acquired,
 * OnlineKMeans computes the new centroids from the weighted average between the original and the
 * estimated centroids. The weight of the estimated centroids is the number of points assigned to
 * them. The weight of the original centroids is also the number of points, but additionally
 * multiplying with the decay factor.
 *
 * <p>The decay factor scales the contribution of the clusters as estimated thus far. If decay
 * factor is 1, all batches are weighted equally. If decay factor is 0, new centroids are determined
 * entirely by recent data. Lower values correspond to more forgetting.
 */
public class OnlineKMeans
        implements Estimator<OnlineKMeans, OnlineKMeansModel>, OnlineKMeansParams<OnlineKMeans> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private Table initModelDataTable;

    public OnlineKMeans() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public OnlineKMeansModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();

        DataStream<DenseVector> points =
                tEnv.toDataStream(inputs[0]).map(new FeaturesExtractor(getFeaturesCol()));

        DataStream<KMeansModelData> initModelData =
                KMeansModelData.getModelDataStream(initModelDataTable);
        initModelData.getTransformation().setParallelism(1);

        IterationBody body =
                new OnlineKMeansIterationBody(
                        DistanceMeasure.getInstance(getDistanceMeasure()),
                        getDecayFactor(),
                        getGlobalBatchSize());

        DataStream<KMeansModelData> finalModelData =
                Iterations.iterateUnboundedStreams(
                                DataStreamList.of(initModelData), DataStreamList.of(points), body)
                        .get(0);

        Table finalModelDataTable = tEnv.fromDataStream(finalModelData);
        OnlineKMeansModel model = new OnlineKMeansModel().setModelData(finalModelDataTable);
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    /** Saves the metadata AND bounded model data table (if exists) to the given path. */
    @Override
    public void save(String path) throws IOException {
        if (initModelDataTable != null) {
            ReadWriteUtils.saveModelData(
                    KMeansModelData.getModelDataStream(initModelDataTable),
                    path,
                    new KMeansModelData.ModelDataEncoder());
        }

        ReadWriteUtils.saveMetadata(this, path);
    }

    public static OnlineKMeans load(StreamExecutionEnvironment env, String path)
            throws IOException {
        OnlineKMeans kMeans = ReadWriteUtils.loadStageParam(path);

        String initModelDataPath = ReadWriteUtils.getDataPath(path);
        if (Files.exists(Paths.get(initModelDataPath))) {
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            DataStream<KMeansModelData> initModelDataStream =
                    ReadWriteUtils.loadModelData(env, path, new KMeansModelData.ModelDataDecoder());

            kMeans.initModelDataTable = tEnv.fromDataStream(initModelDataStream);
        }

        return kMeans;
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    private static class OnlineKMeansIterationBody implements IterationBody {
        private final DistanceMeasure distanceMeasure;
        private final double decayFactor;
        private final int batchSize;

        public OnlineKMeansIterationBody(
                DistanceMeasure distanceMeasure, double decayFactor, int batchSize) {
            this.distanceMeasure = distanceMeasure;
            this.decayFactor = decayFactor;
            this.batchSize = batchSize;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<KMeansModelData> modelData = variableStreams.get(0);
            DataStream<DenseVector> points = dataStreams.get(0);

            int parallelism = points.getParallelism();

            DataStream<KMeansModelData> newModelData =
                    points.countWindowAll(batchSize)
                            .apply(new GlobalBatchCreator())
                            .flatMap(new GlobalBatchSplitter(parallelism))
                            .rebalance()
                            .connect(modelData.broadcast())
                            .transform(
                                    "ModelDataLocalUpdater",
                                    TypeInformation.of(KMeansModelData.class),
                                    new ModelDataLocalUpdater(distanceMeasure))
                            .setParallelism(parallelism)
                            .connect(modelData.broadcast())
                            .transform(
                                    "ModelDataGlobalUpdater",
                                    TypeInformation.of(KMeansModelData.class),
                                    new ModelDataGlobalUpdater(parallelism, decayFactor))
                            .setParallelism(1);

            return new IterationBodyResult(
                    DataStreamList.of(newModelData), DataStreamList.of(modelData));
        }
    }

    private static class ModelDataGlobalUpdater extends AbstractStreamOperator<KMeansModelData>
            implements TwoInputStreamOperator<KMeansModelData, KMeansModelData, KMeansModelData> {
        private final int upstreamParallelism;
        private final double decayFactor;

        private ListState<Integer> localModelDataReceivingState;
        private ListState<Boolean> initModelDataReceivingState;
        private ListState<KMeansModelData> modelDataState;

        private ModelDataGlobalUpdater(int upstreamParallelism, double decayFactor) {
            this.upstreamParallelism = upstreamParallelism;
            this.decayFactor = decayFactor;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            localModelDataReceivingState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "localModelDataReceiving",
                                            BasicTypeInfo.INT_TYPE_INFO));

            initModelDataReceivingState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "initModelDataReceiving",
                                            BasicTypeInfo.BOOLEAN_TYPE_INFO));

            modelDataState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("modelData", KMeansModelData.class));

            initStateValues();
        }

        private void initStateValues() throws Exception {
            localModelDataReceivingState.update(Collections.singletonList(0));
            initModelDataReceivingState.update(Collections.singletonList(false));
            modelDataState.update(Collections.singletonList(new KMeansModelData()));
        }

        @Override
        public void processElement1(StreamRecord<KMeansModelData> localModelDataUpdateRecord)
                throws Exception {
            int localModelDataReceiving =
                    getUniqueElement(localModelDataReceivingState, "localModelDataReceiving");
            Preconditions.checkState(localModelDataReceiving < upstreamParallelism);
            localModelDataReceivingState.update(
                    Collections.singletonList(localModelDataReceiving + 1));
            aggregateWeightedAverageModelData(localModelDataUpdateRecord.getValue());
        }

        @Override
        public void processElement2(StreamRecord<KMeansModelData> initModelDataRecord)
                throws Exception {
            boolean initModelDataReceiving =
                    getUniqueElement(initModelDataReceivingState, "initModelDataReceiving");
            Preconditions.checkState(!initModelDataReceiving);
            initModelDataReceivingState.update(Collections.singletonList(true));
            KMeansModelData initModelData = initModelDataRecord.getValue();
            BLAS.scal(decayFactor, initModelData.weights);
            aggregateWeightedAverageModelData(initModelData);
        }

        private void aggregateWeightedAverageModelData(KMeansModelData newModelData)
                throws Exception {
            KMeansModelData modelData = getUniqueElement(modelDataState, "modelData");
            if (modelData.centroids == null) {
                modelDataState.update(Collections.singletonList(newModelData));
                return;
            }

            DenseVector weights = modelData.weights;
            DenseVector[] centroids = modelData.centroids;
            DenseVector newWeights = newModelData.weights;
            DenseVector[] newCentroids = newModelData.centroids;

            int k = newCentroids.length;
            int dim = newCentroids[0].size();

            for (int i = 0; i < k; i++) {
                for (int j = 0; j < dim; j++) {
                    centroids[i].values[j] =
                            (centroids[i].values[j] * weights.values[i]
                                            + newCentroids[i].values[j] * newWeights.values[i])
                                    / Math.max(weights.values[i] + newWeights.values[i], 1e-16);
                }
                weights.values[i] += newWeights.values[i];
            }

            if (getUniqueElement(initModelDataReceivingState, "initModelDataReceiving")
                    && getUniqueElement(localModelDataReceivingState, "localModelDataReceiving")
                            >= upstreamParallelism) {
                output.collect(new StreamRecord<>(new KMeansModelData(centroids, weights)));
                initStateValues();
            } else {
                modelDataState.update(
                        Collections.singletonList(new KMeansModelData(centroids, weights)));
            }
        }
    }

    private static <T> T getUniqueElement(ListState<T> state, String stateName) throws Exception {
        T value = OperatorStateUtils.getUniqueElement(state, stateName).orElse(null);
        return Objects.requireNonNull(value);
    }

    private static class ModelDataLocalUpdater extends AbstractStreamOperator<KMeansModelData>
            implements TwoInputStreamOperator<DenseVector[], KMeansModelData, KMeansModelData> {
        private final DistanceMeasure distanceMeasure;
        private ListState<DenseVector[]> localBatchState;
        private ListState<KMeansModelData> modelDataState;

        private ModelDataLocalUpdater(DistanceMeasure distanceMeasure) {
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            TypeInformation<DenseVector[]> type =
                    ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE);
            localBatchState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("localBatch", type));

            modelDataState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("modelData", KMeansModelData.class));
        }

        @Override
        public void processElement1(StreamRecord<DenseVector[]> pointsRecord) throws Exception {
            localBatchState.add(pointsRecord.getValue());
            alignAndComputeModelData();
        }

        @Override
        public void processElement2(StreamRecord<KMeansModelData> modelDataRecord)
                throws Exception {
            modelDataState.add(modelDataRecord.getValue());
            alignAndComputeModelData();
        }

        private void alignAndComputeModelData() throws Exception {
            if (!modelDataState.get().iterator().hasNext()
                    || !localBatchState.get().iterator().hasNext()) {
                return;
            }

            DenseVector[] centroids = getUniqueElement(modelDataState, "modelData").centroids;
            modelDataState.clear();

            List<DenseVector[]> pointsList = IteratorUtils.toList(localBatchState.get().iterator());
            DenseVector[] points = pointsList.remove(0);
            localBatchState.update(pointsList);

            int dim = centroids[0].size();
            int k = centroids.length;

            // Computes new centroids.
            DenseVector[] sums = new DenseVector[k];
            DenseVector counts = new DenseVector(k);

            for (int i = 0; i < k; i++) {
                sums[i] = new DenseVector(dim);
                counts.values[i] = 0;
            }
            for (DenseVector point : points) {
                int closestCentroidId =
                        KMeans.findClosestCentroidId(centroids, point, distanceMeasure);
                counts.values[closestCentroidId]++;
                for (int j = 0; j < dim; j++) {
                    sums[closestCentroidId].values[j] += point.values[j];
                }
            }

            for (int i = 0; i < k; i++) {
                if (counts.values[i] < 1e-5) {
                    continue;
                }
                BLAS.scal(1.0 / counts.values[i], sums[i]);
            }

            output.collect(new StreamRecord<>(new KMeansModelData(sums, counts)));
        }
    }

    private static class FeaturesExtractor implements MapFunction<Row, DenseVector> {
        private final String featuresCol;

        private FeaturesExtractor(String featuresCol) {
            this.featuresCol = featuresCol;
        }

        @Override
        public DenseVector map(Row row) throws Exception {
            return (DenseVector) row.getField(featuresCol);
        }
    }

    // An operator that splits a global batch into evenly-sized local batches, and distributes them
    // to downstream operator.
    private static class GlobalBatchSplitter
            implements FlatMapFunction<DenseVector[], DenseVector[]> {
        private final int downStreamParallelism;

        private GlobalBatchSplitter(int downStreamParallelism) {
            this.downStreamParallelism = downStreamParallelism;
        }

        @Override
        public void flatMap(DenseVector[] values, Collector<DenseVector[]> collector) {
            // Calculate the batch sizes to be distributed on each subtask.
            List<Integer> sizes = new ArrayList<>();
            for (int i = 0; i < downStreamParallelism; i++) {
                int start = i * values.length / downStreamParallelism;
                int end = (i + 1) * values.length / downStreamParallelism;
                sizes.add(end - start);
            }

            int offset = 0;
            for (Integer size : sizes) {
                collector.collect(Arrays.copyOfRange(values, offset, offset + size));
                offset += size;
            }
        }
    }

    private static class GlobalBatchCreator
            implements AllWindowFunction<DenseVector, DenseVector[], GlobalWindow> {
        @Override
        public void apply(
                GlobalWindow timeWindow,
                Iterable<DenseVector> iterable,
                Collector<DenseVector[]> collector) {
            List<DenseVector> points = IteratorUtils.toList(iterable.iterator());
            collector.collect(points.toArray(new DenseVector[0]));
        }
    }

    public OnlineKMeans setInitialModelData(Table initModelDataTable) {
        this.initModelDataTable = initModelDataTable;
        return this;
    }

    public OnlineKMeans setRandomCentroids(
            StreamTableEnvironment tEnv, int dim, int k, double weight) {
        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();
        DenseVector[] centroids = new DenseVector[k];
        Random random = new Random(getSeed());
        for (int i = 0; i < k; i++) {
            centroids[i] = new DenseVector(dim);
            for (int j = 0; j < dim; j++) {
                centroids[i].values[j] = random.nextDouble();
            }
        }
        DenseVector weights = new DenseVector(k);
        Arrays.fill(weights.values, weight);
        this.initModelDataTable =
                tEnv.fromDataStream(env.fromElements(new KMeansModelData(centroids, weights)));
        return this;
    }
}
