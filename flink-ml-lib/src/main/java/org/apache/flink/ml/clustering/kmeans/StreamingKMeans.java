package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.Source;
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
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

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
                            .flatMap(new UpdateModelDataOperator(distanceMeasure, decayFactor, timeUnit, K));

            return new IterationBodyResult(
                    DataStreamList.of(newCentroids), DataStreamList.of(newCentroids));
        }
    }

    private static class UpdateModelDataOperator
            implements CoFlatMapFunction<
                    DenseVector[], KMeansModelData, KMeansModelData> {
        private final DistanceMeasure distanceMeasure;
        private final double decayFactor;
        private final String timeUnit;
        private final int K;
        private KMeansModelData modelData;
        Queue<DenseVector[]> pointsBuffer = new ArrayDeque<>();

        public UpdateModelDataOperator(DistanceMeasure distanceMeasure, double decayFactor, String timeUnit, int k) {
            this.distanceMeasure = distanceMeasure;
            this.decayFactor = decayFactor;
            this.timeUnit = timeUnit;
            K = k;
        }

        @Override
        public void flatMap1(
                DenseVector[] points, Collector<KMeansModelData> output) {
            pointsBuffer.add(points);
            flatMap(output);
        }

        @Override
        public void flatMap2(
                KMeansModelData modelData, Collector<KMeansModelData> output) {
            this.modelData = modelData;
            flatMap(output);
        }

        private void flatMap(Collector<KMeansModelData> output) {
            if (modelData == null || pointsBuffer.isEmpty()) {
                return;
            }

            DenseVector[] centroids = modelData.centroids;
            DenseVector weights = modelData.weights;
            DenseVector[] points = pointsBuffer.poll();
            DenseVector[] sums = new DenseVector[K];
            int dims = centroids[0].size();
            int[] counts = new int[K];

            for (int i = 0; i < K; i++) {
                sums[i] = new DenseVector(dims);
                counts[i] = 0;
            }
            for (DenseVector point : points) {
                double minDistance = Double.MAX_VALUE;
                int closestCentroidId = -1;

                for (int j = 0; j < centroids.length; j++) {
                    DenseVector centroid = centroids[j];
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

            BLAS.scal(discount, weights);
            for (int i =0; i < K; i ++) {
                DenseVector centroid = centroids[i];

                double updatedWeight = weights.values[i] + counts[i];
                double lambda = counts[i] / Math.max(updatedWeight, 1e-16);

                weights.values[i] = updatedWeight;
                BLAS.scal(1.0 - lambda, centroid);
                BLAS.axpy(lambda / counts[i], sums[i], centroid);
            }

            output.collect(new KMeansModelData(centroids, weights));
            modelData = null;
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
