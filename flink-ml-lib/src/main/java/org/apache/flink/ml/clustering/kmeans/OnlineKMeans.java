package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.common.param.HasBatchStrategy;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;

public class OnlineKMeans
        implements Estimator<OnlineKMeans, KMeansModel>, OnlineKMeansParams<OnlineKMeans> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private transient DataStream<KMeansModelData> initCentroids;

    public OnlineKMeans() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public KMeansModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        Preconditions.checkArgument(HasBatchStrategy.COUNT_STRATEGY.equals(getBatchStrategy()));
        Preconditions.checkArgument(initCentroids != null || getInitRandomCentroids(),
                "Initial model data needs to be explicitly set or randomly created.");

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();

        DataStream<DenseVector> points =
                tEnv.toDataStream(inputs[0])
                        .map(row -> (DenseVector) row.getField(getFeaturesCol()));

        if (getInitRandomCentroids()) {
            initCentroids = createRandomCentroids(env, getDims(), getK(), getSeed());
        }

        IterationBody body =
                new KMeansIterationBody(DistanceMeasure.getInstance(getDistanceMeasure()), getDecayFactor(), getTimeUnit(), getMaxIter(), getBatchSize(), getK());

        DataStream<DenseVector[]> finalCentroids =
                Iterations.iterateUnboundedStreams(
                                DataStreamList.of(initCentroids), DataStreamList.of(points), body)
                        .get(0);

        Table finalCentroidsTable = tEnv.fromDataStream(finalCentroids);
        KMeansModel model = new KMeansModel().setModelData(finalCentroidsTable);
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static OnlineKMeans load(StreamExecutionEnvironment env, String path)
            throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    private static class KMeansIterationBody implements IterationBody {
        private final DistanceMeasure distanceMeasure;
        private final double decayFactor;
        private final String timeUnit;
        private final int maxIter;
        private final int batchSize;
        private final int K;

        public KMeansIterationBody(DistanceMeasure distanceMeasure, double decayFactor, String timeUnit, int maxIter, int batchSize, int k) {
            this.distanceMeasure = distanceMeasure;
            this.decayFactor = decayFactor;
            this.timeUnit = timeUnit;
            this.maxIter = maxIter;
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
                            .flatMap(new SelectNearestCentroidOperator(distanceMeasure, decayFactor, timeUnit, maxIter, K));

            return new IterationBodyResult(
                    DataStreamList.of(newCentroids), DataStreamList.of(newCentroids));
        }
    }

    private static class SelectNearestCentroidOperator
            implements CoFlatMapFunction<
                    DenseVector[], KMeansModelData, KMeansModelData> {
        private final DistanceMeasure distanceMeasure;
        private final double decayFactor;
        private final String timeUnit;
        private final int maxIter;
        private final int K;
//        private DenseVector[] currentCentroids;
        private KMeansModelData currentModelData;
        Queue<DenseVector[]> pointsBuffer = new ArrayDeque<>();

        public SelectNearestCentroidOperator(DistanceMeasure distanceMeasure, double decayFactor, String timeUnit, int maxIter, int k) {
            this.distanceMeasure = distanceMeasure;
            this.decayFactor = decayFactor;
            this.timeUnit = timeUnit;
            this.maxIter = maxIter;
            K = k;
        }

        @Override
        public void flatMap1(
                DenseVector[] points, Collector<KMeansModelData> output)
                throws Exception {
            pointsBuffer.add(points);
            flatMap(output);
        }

        @Override
        public void flatMap2(
                KMeansModelData modelData, Collector<KMeansModelData> output)
                throws Exception {
            currentModelData = modelData;

            flatMap(output);
        }

        private void flatMap(Collector<KMeansModelData> output) {
            if (currentModelData == null || pointsBuffer.isEmpty()) {
                return;
            }

            DenseVector[] currentCentroids = currentModelData.centroids;
            DenseVector currentWeights = currentModelData.weights;
            DenseVector[] points = pointsBuffer.poll();
            DenseVector[] sums = new DenseVector[K];
            int dims = currentCentroids[0].size();
            int[] counts = new int[K];

            for (int i = 0; i < K; i++) {
                sums[i] = new DenseVector(dims);
                counts[i] = 0;
            }
            for (DenseVector point : points) {
                double minDistance = Double.MAX_VALUE;
                int closestCentroidId = -1;

                for (int j = 0; j < currentCentroids.length; j++) {
                    DenseVector centroid = currentCentroids[j];
                    double distance = distanceMeasure.distance(centroid, point);
                    if (distance < minDistance) {
                        minDistance = distance;
                        closestCentroidId = j;
                    }
                }
                counts[closestCentroidId]++;
                for (int j = 0; j < dims; j++) {
                    sums[closestCentroidId].values[j] += point.values[j];
                }
            }

            double discount = decayFactor;
            if (timeUnit.equals(POINT_UNIT)) {
                discount = Math.pow(decayFactor, points.length);
            }

            BLAS.scal(discount, currentWeights);
            for (int i =0; i < K; i ++) {
                DenseVector centroid = currentCentroids[i];

                double updatedWeight = currentWeights.values[i] + counts[i];
                double lambda = counts[i] / Math.max(updatedWeight, 1e-16);

                currentWeights.values[i] = updatedWeight;
                BLAS.scal(1.0 - lambda, centroid);
                BLAS.axpy(lambda / counts[i], sums[i], centroid);
            }

            output.collect(new KMeansModelData(currentCentroids, currentWeights));
            currentModelData = null;
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
