package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.common.param.HasBatchStrategy;
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
    private transient DataStream<DenseVector[]> initCentroids;

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
                new KMeansIterationBody(DistanceMeasure.getInstance(getDistanceMeasure()), getMaxIter(), getBatchSize(), getK());

        DataStream<DenseVector[]> finalCentroids =
                Iterations.iterateUnboundedStreams(
                                DataStreamList.of(initCentroids), DataStreamList.of(points), body)
                        .get(0);

        Table finalCentroidsTable = tEnv.fromDataStream(finalCentroids.map(KMeansModelData::new));
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
        private final int maxIter;
        private final int batchSize;
        private final int K;

        public KMeansIterationBody(DistanceMeasure distanceMeasure, int maxIter, int batchSize, int k) {
            this.distanceMeasure = distanceMeasure;
            this.maxIter = maxIter;
            this.batchSize = batchSize;
            K = k;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<DenseVector[]> centroidsWithIter = variableStreams.get(0);
            DataStream<DenseVector> points = dataStreams.get(0);

            DataStream<DenseVector[]> newCentroids =
                    points.countWindowAll(batchSize)
                            .aggregate(new MiniBatchCreator())
                            .connect(centroidsWithIter.broadcast())
                            .flatMap(new SelectNearestCentroidOperator(distanceMeasure, maxIter, K));

            return new IterationBodyResult(
                    DataStreamList.of(newCentroids), DataStreamList.of(newCentroids));
        }
    }

    private static class SelectNearestCentroidOperator
            implements CoFlatMapFunction<
                    DenseVector[], DenseVector[], DenseVector[]> {
        private final DistanceMeasure distanceMeasure;
        private final int maxIter;
        private final int K;
        private DenseVector[] currentCentroids;
        Queue<DenseVector[]> pointsBuffer = new ArrayDeque<>();

        public SelectNearestCentroidOperator(DistanceMeasure distanceMeasure, int maxIter, int k) {
            this.distanceMeasure = distanceMeasure;
            this.maxIter = maxIter;
            K = k;
        }

        @Override
        public void flatMap1(
                DenseVector[] points, Collector<DenseVector[]> output)
                throws Exception {
            System.out.println("point");
            pointsBuffer.add(points);
            flatMap(output);
        }

        @Override
        public void flatMap2(
                DenseVector[] centroids, Collector<DenseVector[]> output)
                throws Exception {
            System.out.println("centroidValues ");
            currentCentroids = centroids;

            flatMap(output);
        }

        private void flatMap(Collector<DenseVector[]> output) {
            if (currentCentroids == null || pointsBuffer.isEmpty()) {
                return;
            }

            DenseVector[] points = pointsBuffer.poll();
            DenseVector[] newCentroids = new DenseVector[K];
            int dims = currentCentroids[0].size();
            int[] counts = new int[K];
            int currentIter = 0;
            do {
                for (int i = 0; i < K; i++) {
                    newCentroids[i] = new DenseVector(dims);
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
                        newCentroids[closestCentroidId].values[j] += point.values[j];
                    }
                }
                for (int i = 0; i < K; i++) {
                    if (counts[i] == 0) continue;
                    for (int j = 0; j < dims; j++) {
                        newCentroids[i].values[j] /= counts[i];
                    }
                }

                currentIter++;
            } while (currentIter < maxIter);


            output.collect(newCentroids);
            currentCentroids = null;
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

    private static DataStream<DenseVector[]> createRandomCentroids(StreamExecutionEnvironment env, int dims, int k, long seed) {
        DenseVector[] centroids = new DenseVector[k];
        Random random = new Random(seed);
        for (int i = 0; i < k; i ++) {
            centroids[i] = new DenseVector(dims);
            for (int j = 0; j < dims; j ++) {
                centroids[i].values[j] = random.nextDouble();
            }
        }
        return env.fromCollection(Collections.singletonList(centroids));
    }
}
