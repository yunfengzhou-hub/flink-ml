package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
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
import java.time.Duration;
import java.util.*;

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
    }

    @Override
    public OnlineKMeansModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        Preconditions.checkArgument(HasBatchStrategy.COUNT_STRATEGY.equals(getBatchStrategy()));
        Preconditions.checkArgument(initModelDataTable != null || getInitRandomCentroids(),
                "Initial model data needs to be explicitly set or randomly created.");

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();

        DataStream<DenseVector> points =
                tEnv.toDataStream(inputs[0])
                        .map(new FeaturesExtractor(getFeaturesCol()));

        DataStream<KMeansModelData> initCentroids;
        if (getInitRandomCentroids()) {
            initCentroids = createRandomCentroids(env, getDims(), getK(), getSeed());
        } else {
            initCentroids = KMeansModelData.getModelDataStream(initModelDataTable);
        }

        initCentroids = initCentroids.transform("printoperator", TypeInformation.of(KMeansModelData.class), new PrintOperator());

        IterationBody body =
                new KMeansIterationBody(DistanceMeasure.getInstance(getDistanceMeasure()), getDecayFactor(), getTimeUnit(), getMaxIter(), getBatchSize(), getK());

        DataStream<DenseVector[]> finalCentroids =
                Iterations.iterateUnboundedStreams(
                                DataStreamList.of(initCentroids), DataStreamList.of(points), body)
                        .get(0);

        Table finalCentroidsTable = tEnv.fromDataStream(finalCentroids);
        OnlineKMeansModel model = new OnlineKMeansModel(tEnv.fromDataStream(initCentroids)).setModelData(finalCentroidsTable);
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    public static class PrintOperator extends AbstractStreamOperator<KMeansModelData>
            implements OneInputStreamOperator<KMeansModelData, KMeansModelData>, BoundedOneInput {

        @Override
        public void endInput() throws Exception {
            System.out.println("init model data end input.");
        }

        @Override
        public void processElement(StreamRecord<KMeansModelData> streamRecord) throws Exception {
            System.out.println("offline Kmeans offered model data " + Arrays.toString(streamRecord.getValue().centroids));
            output.collect(streamRecord);
        }
    }

    @Override
    public void save(String path) throws IOException {
        if (initModelDataTable != null) {
            System.out.println("Saving init model data for onlineKMeans");
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

    public static OnlineKMeans load(StreamExecutionEnvironment env, String path)
            throws IOException {
        OnlineKMeans kMeans = ReadWriteUtils.loadStageParam(path);

        Path initModelDataPath = Paths.get(path, "initModelData");

        System.out.println(Arrays.toString(OnlineKMeansModel.getDataPaths(initModelDataPath.toString())));
        if (Files.exists(initModelDataPath)) {
            System.out.println("Loading init model data for onlineKMeans");
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            Source<KMeansModelData, ?, ?> initModelDataSource =
                    FileSource.forRecordStreamFormat(new KMeansModelData.ModelDataDecoder(),
                            new org.apache.flink.core.fs.Path(initModelDataPath.toString())).build();
            DataStream<KMeansModelData> initModelDataStream = env.fromSource(initModelDataSource, WatermarkStrategy.noWatermarks(), "initModelData");

            kMeans.initModelDataTable = tEnv.fromDataStream(initModelDataStream);
        }

        return kMeans;
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
                            .flatMap(new UpdateModelDataOperator(distanceMeasure, decayFactor, timeUnit, maxIter, K));

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
        private final int maxIter;
        private final int K;
//        private DenseVector[] currentCentroids;
        private KMeansModelData currentModelData;
        Queue<DenseVector[]> pointsBuffer = new ArrayDeque<>();

        public UpdateModelDataOperator(DistanceMeasure distanceMeasure, double decayFactor, String timeUnit, int maxIter, int k) {
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
            System.out.println("SelectNearestCentroidOperator.flatMap1");
            pointsBuffer.add(points);
            flatMap(output);
        }

        @Override
        public void flatMap2(
                KMeansModelData modelData, Collector<KMeansModelData> output)
                throws Exception {
            System.out.println("SelectNearestCentroidOperator.flatMap2");
            currentModelData = modelData;

            flatMap(output);
        }

        private void flatMap(Collector<KMeansModelData> output) {
            if (currentModelData == null || pointsBuffer.isEmpty()) {
                return;
            }
            System.out.println("SelectNearestCentroidOperator.flatMap");

            DenseVector[] currentCentroids = currentModelData.centroids;
            DenseVector currentWeights = currentModelData.weights;
            DenseVector[] points = pointsBuffer.poll();
            DenseVector[] sums = new DenseVector[K];
            int dims = currentCentroids[0].size();
            int[] counts = new int[K];

            System.out.println("SelectNearestCentroidOperator.flatMap 1");
            for (int i = 0; i < K; i++) {
                sums[i] = new DenseVector(dims);
                counts[i] = 0;
            }
            System.out.println("SelectNearestCentroidOperator.flatMap 2");
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
            System.out.println("SelectNearestCentroidOperator.flatMap 3");

            double discount = decayFactor;
            if (timeUnit.equals(POINT_UNIT)) {
                discount = Math.pow(decayFactor, points.length);
            }
            System.out.println("SelectNearestCentroidOperator.flatMap 4");
            System.out.println(discount);
            System.out.println(currentWeights);

            BLAS.scal(discount, currentWeights);
            System.out.println("SelectNearestCentroidOperator.flatMap 4.5");
            for (int i =0; i < K; i ++) {
                System.out.println("SelectNearestCentroidOperator.flatMap 4 0");
                DenseVector centroid = currentCentroids[i];

                double updatedWeight = currentWeights.values[i] + counts[i];
                double lambda = counts[i] / Math.max(updatedWeight, 1e-16);
                System.out.println("SelectNearestCentroidOperator.flatMap 4 1");

                currentWeights.values[i] = updatedWeight;
                System.out.println("SelectNearestCentroidOperator.flatMap 4 2");
                BLAS.scal(1.0 - lambda, centroid);
                System.out.println("SelectNearestCentroidOperator.flatMap 4 3");
                BLAS.axpy(lambda / counts[i], sums[i], centroid);
                System.out.println("SelectNearestCentroidOperator.flatMap 4 4");
            }
            System.out.println("SelectNearestCentroidOperator.flatMap 5");

            output.collect(new KMeansModelData(currentCentroids, currentWeights));
            currentModelData = null;

            System.out.println("SelectNearestCentroidOperator.flatMap ends");
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
