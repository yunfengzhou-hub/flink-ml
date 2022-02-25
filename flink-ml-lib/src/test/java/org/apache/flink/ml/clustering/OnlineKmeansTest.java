package org.apache.flink.ml.clustering;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.clustering.kmeans.KMeansModelData;
import org.apache.flink.ml.clustering.kmeans.OnlineKMeans;
import org.apache.flink.ml.common.param.HasDecayFactor;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.util.MockBlockingQueueSinkFunction;
import org.apache.flink.ml.util.MockBlockingQueueSourceFunction;
import org.apache.flink.ml.util.TestBlockingQueueManager;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.*;

import static org.junit.Assert.*;

public class OnlineKmeansTest {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    private String trainId;
    private String predictId;
    private String outputId;
    private String modelDataId;
    private static final DenseVector[] trainData1 =
            new DenseVector[]{
                    Vectors.dense(10.0, 0.0),
                    Vectors.dense(10.0, 0.3),
                    Vectors.dense(10.3, 0.0),
                    Vectors.dense(-10.0, 0.0),
                    Vectors.dense(-10.0, 0.6),
                    Vectors.dense(-10.6, 0.0)};
    private static final DenseVector[] trainData2 =
            new DenseVector[]{
                    Vectors.dense(10.0, 100.0),
                    Vectors.dense(10.0, 100.3),
                    Vectors.dense(10.3, 100.0),
                    Vectors.dense(-10.0, -100.0),
                    Vectors.dense(-10.0, -100.6),
                    Vectors.dense(-10.6, -100.0)};
    private static final DenseVector[] predictData =
            new DenseVector[]{
                    Vectors.dense(10.0, 10.0),
                    Vectors.dense(-10.0, 10.0)};
    private static final List<Set<DenseVector>> expectedGroups1 =
            Arrays.asList(
                    new HashSet<>(
                            Collections.singletonList(Vectors.dense(10.0, 10.0))),
                    new HashSet<>(
                            Collections.singletonList(Vectors.dense(-10.0, 10.0))));
    private static final List<Set<DenseVector>> expectedGroups2 =
            Collections.singletonList(
                    new HashSet<>(
                            Arrays.asList(
                                    Vectors.dense(10.0, 10.0),
                                    Vectors.dense(-10.0, 10.0))));
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private Table trainTable;
    private Table predictTable;

    @Before
    public void before() {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);

        trainId = TestBlockingQueueManager.createBlockingQueue();
        predictId = TestBlockingQueueManager.createBlockingQueue();
        outputId = TestBlockingQueueManager.createBlockingQueue();
        modelDataId = TestBlockingQueueManager.createBlockingQueue();

        Schema schema = Schema.newBuilder().column("f0", DataTypes.of(DenseVector.class)).build();
        trainTable =
                tEnv.fromDataStream(
                                env.addSource(
                                        new MockBlockingQueueSourceFunction<>(trainId),
                                        DenseVectorTypeInfo.INSTANCE),
                                schema)
                        .as("features");
        predictTable =
                tEnv.fromDataStream(
                                env.addSource(
                                        new MockBlockingQueueSourceFunction<>(predictId),
                                        DenseVectorTypeInfo.INSTANCE),
                                schema)
                        .as("features");
    }

    private static List<Set<DenseVector>> groupFeaturesByPrediction(
            List<Row> rows, String featureCol, String predictionCol) {
        Map<Integer, Set<DenseVector>> map = new HashMap<>();
        for (Row row: rows) {
            DenseVector vector = (DenseVector) row.getField(featureCol);
            int predict = (Integer) row.getField(predictionCol);
            map.putIfAbsent(predict, new HashSet<>());
            map.get(predict).add(vector);
        }
        return new ArrayList<>(map.values());
    }

    @Test
    public void testOnlineFitAndPredict() throws Exception {
        OnlineKMeans kmeans =
                new OnlineKMeans()
                        .setInitRandomCentroids(true)
                        .setDims(2)
                        .setBatchSize(6)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        KMeansModel model = kmeans.fit(trainTable);
        Table outputTable = model.transform(predictTable)[0];

        DataStream<Row> output = tEnv.toDataStream(outputTable);
        output.addSink(new MockBlockingQueueSinkFunction<>(outputId));

        DataStream<DenseVector[]> modelDataStream = model.getLatestModelData();
        modelDataStream.addSink(new MockBlockingQueueSinkFunction<>(modelDataId));

        JobClient client = env.executeAsync();

        TestBlockingQueueManager.offerAll(trainId, trainData1);
        TestBlockingQueueManager.poll(modelDataId);
        TestBlockingQueueManager.offerAll(predictId, predictData);

        List<Row> rawResult1 = TestBlockingQueueManager.poll(outputId, predictData.length);
        List<Set<DenseVector>> actualGroups1 = groupFeaturesByPrediction(rawResult1, kmeans.getFeaturesCol(), kmeans.getPredictionCol());
        assertTrue(CollectionUtils.isEqualCollection(expectedGroups1, actualGroups1));

        TestBlockingQueueManager.offerAll(trainId, trainData2);
        TestBlockingQueueManager.poll(modelDataId);
        TestBlockingQueueManager.offerAll(predictId, predictData);

        List<Row> rawResult2 = TestBlockingQueueManager.poll(outputId, predictData.length);
        List<Set<DenseVector>> actualGroups2 = groupFeaturesByPrediction(rawResult2, kmeans.getFeaturesCol(), kmeans.getPredictionCol());
        assertTrue(CollectionUtils.isEqualCollection(expectedGroups2, actualGroups2));

        client.cancel();
    }

    @Test
    public void testDecayFactor() throws Exception {
        OnlineKMeans kmeans =
                new OnlineKMeans()
                        .setDecayFactor(100.0)
                        .setInitRandomCentroids(true)
                        .setDims(2)
                        .setBatchSize(6)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        KMeansModel model = kmeans.fit(trainTable);
        Table outputTable = model.transform(predictTable)[0];

        DataStream<Row> output = tEnv.toDataStream(outputTable);
        output.addSink(new MockBlockingQueueSinkFunction<>(outputId));

        DataStream<DenseVector[]> modelDataStream = model.getLatestModelData();
        modelDataStream.addSink(new MockBlockingQueueSinkFunction<>(modelDataId));

        JobClient client = env.executeAsync();

        TestBlockingQueueManager.offerAll(trainId, trainData1);
        TestBlockingQueueManager.poll(modelDataId);
        TestBlockingQueueManager.offerAll(predictId, predictData);

        List<Row> rawResult1 = TestBlockingQueueManager.poll(outputId, predictData.length);
        List<Set<DenseVector>> actualGroups1 = groupFeaturesByPrediction(rawResult1, kmeans.getFeaturesCol(), kmeans.getPredictionCol());
        assertTrue(CollectionUtils.isEqualCollection(expectedGroups1, actualGroups1));

        TestBlockingQueueManager.offerAll(trainId, trainData2);
        TestBlockingQueueManager.poll(modelDataId);
        TestBlockingQueueManager.offerAll(predictId, predictData);

        List<Row> rawResult2 = TestBlockingQueueManager.poll(outputId, predictData.length);
        List<Set<DenseVector>> actualGroups2 = groupFeaturesByPrediction(rawResult2, kmeans.getFeaturesCol(), kmeans.getPredictionCol());
        assertTrue(CollectionUtils.isEqualCollection(expectedGroups1, actualGroups2));

        client.cancel();
    }

    @Test
    public void testHalfLife() throws Exception {
        OnlineKMeans kmeans =
                new OnlineKMeans()
                        .setHalfLife(1, HasDecayFactor.POINT_UNIT)
                        .setInitRandomCentroids(true)
                        .setDims(2)
                        .setBatchSize(6)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        KMeansModel model = kmeans.fit(trainTable);
        Table outputTable = model.transform(predictTable)[0];

        DataStream<Row> output = tEnv.toDataStream(outputTable);
        output.addSink(new MockBlockingQueueSinkFunction<>(outputId));

        DataStream<DenseVector[]> modelDataStream = model.getLatestModelData();
        modelDataStream.addSink(new MockBlockingQueueSinkFunction<>(modelDataId));

        JobClient client = env.executeAsync();

        TestBlockingQueueManager.offerAll(trainId, trainData1);
        TestBlockingQueueManager.poll(modelDataId);
        TestBlockingQueueManager.offerAll(predictId, predictData);

        List<Row> rawResult1 = TestBlockingQueueManager.poll(outputId, predictData.length);
        List<Set<DenseVector>> actualGroups1 = groupFeaturesByPrediction(rawResult1, kmeans.getFeaturesCol(), kmeans.getPredictionCol());
        assertTrue(CollectionUtils.isEqualCollection(expectedGroups1, actualGroups1));

        TestBlockingQueueManager.offerAll(trainId, trainData2);
        TestBlockingQueueManager.poll(modelDataId);
        TestBlockingQueueManager.offerAll(predictId, predictData);

        List<Row> rawResult2 = TestBlockingQueueManager.poll(outputId, predictData.length);
        List<Set<DenseVector>> actualGroups2 = groupFeaturesByPrediction(rawResult2, kmeans.getFeaturesCol(), kmeans.getPredictionCol());
        assertTrue(CollectionUtils.isEqualCollection(expectedGroups2, actualGroups2));

        client.cancel();
    }

    @After
    public void after() {
        TestBlockingQueueManager.deleteBlockingQueue(trainId);
        TestBlockingQueueManager.deleteBlockingQueue(predictId);
        TestBlockingQueueManager.deleteBlockingQueue(outputId);
        TestBlockingQueueManager.deleteBlockingQueue(modelDataId);
    }
}
