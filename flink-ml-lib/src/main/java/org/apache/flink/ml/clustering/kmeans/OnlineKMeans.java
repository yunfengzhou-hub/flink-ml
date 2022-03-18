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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.common.param.HasBatchStrategy;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    public OnlineKMeans(Table... initModelDataTables) {
        Preconditions.checkArgument(initModelDataTables.length == 1);
        this.initModelDataTable = initModelDataTables[0];
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
        setInitMode("direct");
    }

    @Override
    public OnlineKMeansModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        Preconditions.checkArgument(HasBatchStrategy.COUNT_STRATEGY.equals(getBatchStrategy()));

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();

        DataStream<DenseVector> points =
                tEnv.toDataStream(inputs[0]).map(new FeaturesExtractor(getFeaturesCol()));
        points.getTransformation().setParallelism(1);

        DataStream<KMeansModelData> initModelDataStream;
        if (getInitMode().equals("random")) {
            Preconditions.checkState(initModelDataTable == null);
            initModelDataStream = createRandomCentroids(env, getDims(), getK(), getSeed());
        } else {
            initModelDataStream = KMeansModelData.getModelDataStream(initModelDataTable);
        }
        DataStream<Tuple2<KMeansModelData, DenseVector>> initModelDataWithWeightsStream =
                initModelDataStream.map(new InitWeightAssigner(getInitWeights()));
        initModelDataWithWeightsStream.getTransformation().setParallelism(1);

        IterationBody body =
                new OnlineKMeansIterationBody(
                        DistanceMeasure.getInstance(getDistanceMeasure()),
                        getDecayFactor(),
                        getBatchSize(),
                        getK());

        DataStream<KMeansModelData> finalModelDataStream =
                Iterations.iterateUnboundedStreams(
                                DataStreamList.of(initModelDataWithWeightsStream),
                                DataStreamList.of(points),
                                body)
                        .get(0);
        finalModelDataStream = finalModelDataStream.union(initModelDataStream);

        Table finalModelDataTable = tEnv.fromDataStream(finalModelDataStream);
        OnlineKMeansModel model = new OnlineKMeansModel().setModelData(finalModelDataTable);
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    private static class InitWeightAssigner
            implements MapFunction<KMeansModelData, Tuple2<KMeansModelData, DenseVector>> {
        private final double[] initWeights;

        private InitWeightAssigner(Double[] initWeights) {
            this.initWeights = ArrayUtils.toPrimitive(initWeights);
        }

        @Override
        public Tuple2<KMeansModelData, DenseVector> map(KMeansModelData modelData)
                throws Exception {
            return Tuple2.of(modelData, Vectors.dense(initWeights));
        }
    }

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

        Path initModelDataPath = Paths.get(path, "data");
        if (Files.exists(initModelDataPath)) {
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            DataStream<KMeansModelData> initModelDataStream =
                    ReadWriteUtils.loadModelData(env, path, new KMeansModelData.ModelDataDecoder());

            kMeans.initModelDataTable = tEnv.fromDataStream(initModelDataStream);
            kMeans.setInitMode("direct");
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
        private final int k;

        public OnlineKMeansIterationBody(
                DistanceMeasure distanceMeasure, double decayFactor, int batchSize, int k) {
            this.distanceMeasure = distanceMeasure;
            this.decayFactor = decayFactor;
            this.batchSize = batchSize;
            this.k = k;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<Tuple2<KMeansModelData, DenseVector>> modelDataWithWeights =
                    variableStreams.get(0);
            DataStream<DenseVector> points = dataStreams.get(0);

            DataStream<Tuple2<KMeansModelData, DenseVector>> newModelDataWithWeights =
                    points.countWindowAll(batchSize)
                            .aggregate(new MiniBatchCreator())
                            .connect(modelDataWithWeights.broadcast())
                            .transform(
                                    "UpdateModelData",
                                    new TupleTypeInfo<>(
                                            TypeInformation.of(KMeansModelData.class),
                                            DenseVectorTypeInfo.INSTANCE),
                                    new UpdateModelDataOperator(distanceMeasure, decayFactor, k))
                            .setParallelism(1);

            DataStream<KMeansModelData> newModelData =
                    newModelDataWithWeights.map(
                            (MapFunction<Tuple2<KMeansModelData, DenseVector>, KMeansModelData>)
                                    x -> x.f0);

            return new IterationBodyResult(
                    DataStreamList.of(newModelDataWithWeights), DataStreamList.of(newModelData));
        }
    }

    // TODO: change this single-threaded implementation to support training in a distributed way,
    // after model data
    // version mechanism is implemented.
    private static class UpdateModelDataOperator
            extends AbstractStreamOperator<Tuple2<KMeansModelData, DenseVector>>
            implements TwoInputStreamOperator<
                    DenseVector[],
                    Tuple2<KMeansModelData, DenseVector>,
                    Tuple2<KMeansModelData, DenseVector>> {
        private final DistanceMeasure distanceMeasure;
        private final double decayFactor;
        private final int k;
        private ListState<DenseVector[]> miniBatchState;
        private ListState<KMeansModelData> modelDataState;
        private ListState<DenseVector> weightsState;

        public UpdateModelDataOperator(DistanceMeasure distanceMeasure, double decayFactor, int k) {
            this.distanceMeasure = distanceMeasure;
            this.decayFactor = decayFactor;
            this.k = k;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            TypeInformation<DenseVector[]> type =
                    ObjectArrayTypeInfo.getInfoFor(DenseVectorTypeInfo.INSTANCE);
            miniBatchState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("miniBatch", type));

            modelDataState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("modelData", KMeansModelData.class));

            weightsState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "weights", DenseVectorTypeInfo.INSTANCE));
        }

        @Override
        public void processElement1(StreamRecord<DenseVector[]> streamRecord) throws Exception {
            miniBatchState.add(streamRecord.getValue());
            processElement();
        }

        @Override
        public void processElement2(StreamRecord<Tuple2<KMeansModelData, DenseVector>> streamRecord)
                throws Exception {
            modelDataState.add(streamRecord.getValue().f0);
            weightsState.add(streamRecord.getValue().f1);
            processElement();
        }

        private void processElement() throws Exception {
            if (!modelDataState.get().iterator().hasNext()
                    || !miniBatchState.get().iterator().hasNext()) {
                return;
            }

            // Retrieves data from states.
            List<KMeansModelData> modelDataList =
                    IteratorUtils.toList(modelDataState.get().iterator());
            if (modelDataList.size() != 1) {
                throw new RuntimeException(
                        "The operator received "
                                + modelDataList.size()
                                + " list of model data in this round");
            }
            DenseVector[] centroids = modelDataList.get(0).centroids;
            modelDataState.clear();

            List<DenseVector> weightsList = IteratorUtils.toList(weightsState.get().iterator());
            if (weightsList.size() != 1) {
                throw new RuntimeException(
                        "The operator received "
                                + weightsList.size()
                                + " list of weights in this round");
            }
            DenseVector weights = weightsList.get(0);
            weightsState.clear();

            List<DenseVector[]> pointsList = IteratorUtils.toList(miniBatchState.get().iterator());
            DenseVector[] points = pointsList.get(0);
            pointsList.remove(0);
            miniBatchState.clear();
            miniBatchState.addAll(pointsList);

            // Computes new centroids.
            DenseVector[] sums = new DenseVector[k];
            int dims = centroids[0].size();
            int[] counts = new int[k];

            for (int i = 0; i < k; i++) {
                sums[i] = new DenseVector(dims);
                counts[i] = 0;
            }
            for (DenseVector point : points) {
                int closestCentroidId =
                        KMeans.findClosestCentroidId(centroids, point, distanceMeasure);
                counts[closestCentroidId]++;
                for (int j = 0; j < dims; j++) {
                    sums[closestCentroidId].values[j] += point.values[j];
                }
            }

            // Considers weight and decay factor when updating centroids.
            BLAS.scal(decayFactor, weights);
            for (int i = 0; i < k; i++) {
                DenseVector centroid = centroids[i];

                double updatedWeight = weights.values[i] + counts[i];
                double lambda = counts[i] / Math.max(updatedWeight, 1e-16);

                weights.values[i] = updatedWeight;
                BLAS.scal(1.0 - lambda, centroid);
                BLAS.axpy(lambda / counts[i], sums[i], centroid);
            }

            output.collect(new StreamRecord<>(Tuple2.of(new KMeansModelData(centroids), weights)));
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

    private static class MiniBatchCreator
            implements AggregateFunction<DenseVector, List<DenseVector>, DenseVector[]> {
        @Override
        public List<DenseVector> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<DenseVector> add(DenseVector value, List<DenseVector> acc) {
            acc.add(value);
            return acc;
        }

        @Override
        public DenseVector[] getResult(List<DenseVector> acc) {
            return acc.toArray(new DenseVector[0]);
        }

        @Override
        public List<DenseVector> merge(List<DenseVector> acc, List<DenseVector> acc1) {
            acc.addAll(acc1);
            return acc;
        }
    }

    private static DataStream<KMeansModelData> createRandomCentroids(
            StreamExecutionEnvironment env, int dims, int k, long seed) {
        DenseVector[] centroids = new DenseVector[k];
        Random random = new Random(seed);
        for (int i = 0; i < k; i++) {
            centroids[i] = new DenseVector(dims);
            for (int j = 0; j < dims; j++) {
                centroids[i].values[j] = random.nextDouble();
            }
        }
        return env.fromElements(new KMeansModelData(centroids));
    }
}
