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

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.common.param.HasBatchStrategy;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * StreamingKMeans extends the function of {@link KMeans}, supporting to train a K-Means model
 * continuously according to an unbounded stream of train data.
 */
public class StreamingKMeans
        implements Estimator<StreamingKMeans, StreamingKMeansModel>, StreamingKMeansParams<StreamingKMeans> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private Table initModelDataTable;

    public StreamingKMeans() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    public StreamingKMeans(Table... initModelDataTables) {
        Preconditions.checkArgument(initModelDataTables.length == 1);
        this.initModelDataTable = initModelDataTables[0];
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
        setInitMode("direct");
    }

    @Override
    public StreamingKMeansModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        Preconditions.checkArgument(HasBatchStrategy.COUNT_STRATEGY.equals(getBatchStrategy()));
        Preconditions.checkArgument(initModelDataTable != null || getInitMode().equals("random"),
                "Initial model data needs to be directly set or randomly created.");

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();

        DataStream<DenseVector> points =
                tEnv.toDataStream(inputs[0])
                        .map(new FeaturesExtractor(getFeaturesCol()));

        DataStream<KMeansModelData> initCentroids;
        if (getInitMode().equals("random")) {
            initCentroids = createRandomCentroids(env, getDims(), getK(), getSeed());
        } else {
            initCentroids = KMeansModelData.getModelDataStream(initModelDataTable);
        }

        IterationBody body =
                new StreamingKMeansIterationBody(DistanceMeasure.getInstance(getDistanceMeasure()), getDecayFactor(), getTimeUnit(), getBatchSize(), getK());

        DataStream<DenseVector[]> finalCentroids =
                Iterations.iterateUnboundedStreams(
                                DataStreamList.of(initCentroids), DataStreamList.of(points), body)
                        .get(0);

        Table finalCentroidsTable = tEnv.fromDataStream(finalCentroids);
        StreamingKMeansModel model = new StreamingKMeansModel(tEnv.fromDataStream(initCentroids)).setModelData(finalCentroidsTable);
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    @Override
    public void save(String path) throws IOException {
        if (initModelDataTable != null) {
            String initModelDataPath = Paths.get(path, "initModelData").toString();
            FileSink<KMeansModelData> initModelDataSink =
                    FileSink.forRowFormat(
                                    new org.apache.flink.core.fs.Path(initModelDataPath),
                                    new KMeansModelData.ModelDataEncoder())
                            .withRollingPolicy(OnCheckpointRollingPolicy.build())
                            .withBucketAssigner(new BasePathBucketAssigner<>())
                            .build();
            KMeansModelData.getModelDataStream(initModelDataTable).sinkTo(initModelDataSink);
        }

        ReadWriteUtils.saveMetadata(this, path);
    }

    public static StreamingKMeans load(StreamExecutionEnvironment env, String path)
            throws IOException {
        StreamingKMeans kMeans = ReadWriteUtils.loadStageParam(path);

        Path initModelDataPath = Paths.get(path, "initModelData");
        if (Files.exists(initModelDataPath)) {
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            Source<KMeansModelData, ?, ?> initModelDataSource =
                    FileSource.forRecordStreamFormat(new KMeansModelData.ModelDataDecoder(),
                            new org.apache.flink.core.fs.Path(initModelDataPath.toString())).build();
            DataStream<KMeansModelData> initModelDataStream = env.fromSource(initModelDataSource, WatermarkStrategy.noWatermarks(), "initModelData");

            kMeans.initModelDataTable = tEnv.fromDataStream(initModelDataStream);
            kMeans.setInitMode("direct");
        }

        return kMeans;
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    private static class StreamingKMeansIterationBody implements IterationBody {
        private final DistanceMeasure distanceMeasure;
        private final double decayFactor;
        private final String timeUnit;
        private final int batchSize;
        private final int K;

        public StreamingKMeansIterationBody(DistanceMeasure distanceMeasure, double decayFactor, String timeUnit, int batchSize, int k) {
            this.distanceMeasure = distanceMeasure;
            this.decayFactor = decayFactor;
            this.timeUnit = timeUnit;
            this.batchSize = batchSize;
            K = k;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<KMeansModelData> modelData = variableStreams.get(0);
            DataStream<DenseVector> points = dataStreams.get(0);

            DataStream<KMeansModelData> newCentroids =
                    points.countWindowAll(batchSize)
                            .aggregate(new MiniBatchCreator())
                            .connect(modelData.broadcast())
                            .transform(
                                    "UpdateModelData",
                                    TypeInformation.of(KMeansModelData.class),
                                    new UpdateModelDataOperator(distanceMeasure, decayFactor, timeUnit, K)
                            );

            return new IterationBodyResult(
                    DataStreamList.of(newCentroids), DataStreamList.of(newCentroids));
        }
    }

    private static class UpdateModelDataOperator extends AbstractStreamOperator<KMeansModelData>
            implements TwoInputStreamOperator<DenseVector[], KMeansModelData, KMeansModelData> {
        private final DistanceMeasure distanceMeasure;
        private final double decayFactor;
        private final String timeUnit;
        private final int K;
        private ListState<DenseVector[]> miniBatchState;
        private ListState<KMeansModelData> modelDataState;

        public UpdateModelDataOperator(DistanceMeasure distanceMeasure, double decayFactor, String timeUnit, int K) {
            this.distanceMeasure = distanceMeasure;
            this.decayFactor = decayFactor;
            this.timeUnit = timeUnit;
            this.K = K;
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
                            .getListState(new ListStateDescriptor<>("modelData", KMeansModelData.class));
        }

        @Override
        public void processElement1(StreamRecord<DenseVector[]> streamRecord) throws Exception {
            miniBatchState.add(streamRecord.getValue());
            processElement(output);
        }

        @Override
        public void processElement2(StreamRecord<KMeansModelData> streamRecord) throws Exception {
            modelDataState.add(streamRecord.getValue());
            processElement(output);
        }

        private void processElement(Output<StreamRecord<KMeansModelData>> output) throws Exception {
            if (!modelDataState.get().iterator().hasNext() || !miniBatchState.get().iterator().hasNext()) {
                return;
            }

            List<KMeansModelData> modelDataList = IteratorUtils.toList(modelDataState.get().iterator());
            if (modelDataList.size() != 1) {
                throw new RuntimeException(
                        "The operator received "
                                + modelDataList.size()
                                + " list of centroids in this round");
            }
            DenseVector[] centroids = modelDataList.get(0).centroids;
            DenseVector weights = modelDataList.get(0).weights;
            modelDataState.clear();

            List<DenseVector[]> pointsList = IteratorUtils.toList(miniBatchState.get().iterator());
            DenseVector[] points = pointsList.get(0);
            pointsList.remove(0);
            miniBatchState.clear();
            miniBatchState.addAll(pointsList);

            DenseVector[] sums = new DenseVector[K];
            int dims = centroids[0].size();
            int[] counts = new int[K];

            for (int i = 0; i < K; i++) {
                sums[i] = new DenseVector(dims);
                counts[i] = 0;
            }
            for (DenseVector point : points) {
                int closestCentroidId = KMeans.findClosestCentroidId(centroids, point, distanceMeasure);
                counts[closestCentroidId]++;
                for (int j = 0; j < dims; j++) {
                    sums[closestCentroidId].values[j] += point.values[j];
                }
            }

            double discount = decayFactor;
            if (timeUnit.equals(POINT_UNIT)) {
                discount = Math.pow(decayFactor, points.length);
            }

            BLAS.scal(discount, weights);
            for (int i =0; i < K; i ++) {
                DenseVector centroid = centroids[i];

                double updatedWeight = weights.values[i] + counts[i];
                double lambda = counts[i] / Math.max(updatedWeight, 1e-16);

                weights.values[i] = updatedWeight;
                BLAS.scal(1.0 - lambda, centroid);
                BLAS.axpy(lambda / counts[i], sums[i], centroid);
            }

            output.collect(new StreamRecord<>(new KMeansModelData(centroids, weights)));
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

    private static class MiniBatchCreator implements AggregateFunction<DenseVector, List<DenseVector>, DenseVector[]> {
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

    private static DataStream<KMeansModelData> createRandomCentroids(StreamExecutionEnvironment env, int dims, int k, long seed) {
        DenseVector[] centroids = new DenseVector[k];
        Random random = new Random(seed);
        for (int i = 0; i < k; i ++) {
            centroids[i] = new DenseVector(dims);
            for (int j = 0; j < dims; j ++) {
                centroids[i].values[j] = random.nextDouble();
            }
        }
        return env.fromElements(new KMeansModelData(centroids, new DenseVector(k)));
    }
}
