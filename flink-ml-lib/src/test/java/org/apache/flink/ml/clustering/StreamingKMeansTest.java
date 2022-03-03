package org.apache.flink.ml.clustering;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.ml.clustering.kmeans.*;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.util.*;
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

import static org.apache.flink.ml.clustering.KMeansTest.groupFeaturesByPrediction;
import static org.junit.Assert.*;

public class StreamingKMeansTest {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    public static final DenseVector[] trainData1 =
            new DenseVector[]{
                    Vectors.dense(10.0, 0.0),
                    Vectors.dense(10.0, 0.3),
                    Vectors.dense(10.3, 0.0),
                    Vectors.dense(-10.0, 0.0),
                    Vectors.dense(-10.0, 0.6),
                    Vectors.dense(-10.6, 0.0)};
    public static final DenseVector[] trainData2 =
            new DenseVector[]{
                    Vectors.dense(10.0, 100.0),
                    Vectors.dense(10.0, 100.3),
                    Vectors.dense(10.3, 100.0),
                    Vectors.dense(-10.0, -100.0),
                    Vectors.dense(-10.0, -100.6),
                    Vectors.dense(-10.6, -100.0)};
    public static final DenseVector[] predictData =
            new DenseVector[]{
                    Vectors.dense(10.0, 10.0),
                    Vectors.dense(-10.0, 10.0)};
    public static final List<Set<DenseVector>> expectedGroups1 =
            Arrays.asList(
                    new HashSet<>(
                            Collections.singletonList(Vectors.dense(10.0, 10.0))),
                    new HashSet<>(
                            Collections.singletonList(Vectors.dense(-10.0, 10.0))));
    public static final List<Set<DenseVector>> expectedGroups2 =
            Collections.singletonList(
                    new HashSet<>(
                            Arrays.asList(
                                    Vectors.dense(10.0, 10.0),
                                    Vectors.dense(-10.0, 10.0))));

    private String trainId;
    private String predictId;
    private String outputId;
    private String modelDataId;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private Table offlineTrainTable;
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

        offlineTrainTable = tEnv.fromDataStream(env.fromElements(trainData1), schema).as("features");
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

    @Test
    public void testOnlineFitAndPredict() throws Exception {
        KMeans offlineKMeans =
                new KMeans().setFeaturesCol("features").setPredictionCol("prediction");
        KMeansModel offlineModel = offlineKMeans.fit(offlineTrainTable);

        StreamingKMeans streamingKMeans =
                new StreamingKMeans(offlineModel.getModelData())
                        .setDims(2)
                        .setBatchSize(6);
        ReadWriteUtils.updateExistingParams(streamingKMeans, offlineKMeans.getParamMap());
        StreamingKMeansModel streamingModel = streamingKMeans.fit(trainTable);
        Table outputTable = streamingModel.transform(predictTable)[0];

        DataStream<Row> output = tEnv.toDataStream(outputTable);
        output.addSink(new MockBlockingQueueSinkFunction<>(outputId));

        DataStream<DenseVector[]> modelDataStream = streamingModel.getLatestModelData();
        modelDataStream.addSink(new MockBlockingQueueSinkFunction<>(modelDataId));

        JobClient client = env.executeAsync();

        Thread.sleep(5000);

        TestBlockingQueueManager.offerAll(predictId, predictData);
        TestBlockingQueueManager.poll(modelDataId);

        List<Row> rawResult1 = TestBlockingQueueManager.poll(outputId, predictData.length);
        List<Set<DenseVector>> actualGroups1 = groupFeaturesByPrediction(rawResult1, streamingKMeans.getFeaturesCol(), streamingKMeans.getPredictionCol());
        assertTrue(CollectionUtils.isEqualCollection(expectedGroups1, actualGroups1));

        TestBlockingQueueManager.offerAll(trainId, trainData2);
        TestBlockingQueueManager.poll(modelDataId);
        TestBlockingQueueManager.offerAll(predictId, predictData);

        List<Row> rawResult2 = TestBlockingQueueManager.poll(outputId, predictData.length);
        List<Set<DenseVector>> actualGroups2 = groupFeaturesByPrediction(rawResult2, streamingKMeans.getFeaturesCol(), streamingKMeans.getPredictionCol());
        assertTrue(CollectionUtils.isEqualCollection(expectedGroups2, actualGroups2));

        client.cancel();
    }

    @Test
    public void testSaveAndReload() throws Exception {
        KMeans offlineKMeans =
                new KMeans().setFeaturesCol("features").setPredictionCol("prediction");
        KMeansModel offlineModel = offlineKMeans.fit(offlineTrainTable);

        StreamingKMeans streamingKMeans =
                new StreamingKMeans(offlineModel.getModelData())
                        .setDims(2)
                        .setBatchSize(6);
        ReadWriteUtils.updateExistingParams(streamingKMeans, offlineKMeans.getParamMap());

        String kMeansSavePath = tempFolder.newFolder().getAbsolutePath();
        streamingKMeans.save(kMeansSavePath);
        JobClient client1 = env.executeAsync();
        Thread.sleep(5000);
        StreamingKMeans loadedKMeans = StreamingKMeans.load(env, kMeansSavePath);

        StreamingKMeansModel streamingModel = loadedKMeans.fit(trainTable);

        String modelSavePath = tempFolder.newFolder().getAbsolutePath();
        streamingModel.save(modelSavePath);
        JobClient client2 = env.executeAsync();
        Thread.sleep(2000);
        StreamingKMeansModel loadedModel = StreamingKMeansModel.load(env, modelSavePath);

        Table outputTable = loadedModel.transform(predictTable)[0];

        DataStream<Row> output = tEnv.toDataStream(outputTable);
        output.addSink(new MockBlockingQueueSinkFunction<>(outputId));

        DataStream<DenseVector[]> modelDataStream = loadedModel.getLatestModelData();
        modelDataStream.addSink(new MockBlockingQueueSinkFunction<>(modelDataId));

        JobClient client = env.executeAsync();

        Thread.sleep(2000);

        TestBlockingQueueManager.offerAll(predictId, predictData);
        TestBlockingQueueManager.poll(modelDataId);
        TestBlockingQueueManager.poll(outputId, predictData.length);

        TestBlockingQueueManager.offerAll(trainId, trainData2);
        TestBlockingQueueManager.poll(modelDataId);
        TestBlockingQueueManager.offerAll(predictId, predictData);

        List<Row> rawResult2 = TestBlockingQueueManager.poll(outputId, predictData.length);
        List<Set<DenseVector>> actualGroups2 = groupFeaturesByPrediction(rawResult2, streamingKMeans.getFeaturesCol(), streamingKMeans.getPredictionCol());
        assertTrue(CollectionUtils.isEqualCollection(expectedGroups2, actualGroups2));

        try {
            client1.cancel();
            client2.cancel();
            client.cancel();
        } catch (RuntimeException e) {
            if (!e.getMessage().equals("MiniCluster is not yet running or has already been shut down.")) {
                throw e;
            }
        }
    }

    @After
    public void after() {
        TestBlockingQueueManager.deleteBlockingQueue(trainId);
        TestBlockingQueueManager.deleteBlockingQueue(predictId);
        TestBlockingQueueManager.deleteBlockingQueue(outputId);
        TestBlockingQueueManager.deleteBlockingQueue(modelDataId);
    }
}

// test init random centroids


//    @Test
//    public void testDecayFactor() throws Exception {
//        OnlineKMeans kmeans =
//                new OnlineKMeans()
//                        .setDecayFactor(100.0)
//                        .setInitRandomCentroids(true)
//                        .setDims(2)
//                        .setBatchSize(6)
//                        .setFeaturesCol("features")
//                        .setPredictionCol("prediction");
//        KMeansModel model = kmeans.fit(trainTable);
//        Table outputTable = model.transform(predictTable)[0];
//
//        DataStream<Row> output = tEnv.toDataStream(outputTable);
//        output.addSink(new MockBlockingQueueSinkFunction<>(outputId));
//
//        DataStream<DenseVector[]> modelDataStream = model.getLatestModelData();
//        modelDataStream.addSink(new MockBlockingQueueSinkFunction<>(modelDataId));
//
//        JobClient client = env.executeAsync();
//
//        TestBlockingQueueManager.offerAll(trainId, trainData1);
//        TestBlockingQueueManager.poll(modelDataId);
//        TestBlockingQueueManager.offerAll(predictId, predictData);
//
//        List<Row> rawResult1 = TestBlockingQueueManager.poll(outputId, predictData.length);
//        List<Set<DenseVector>> actualGroups1 = groupFeaturesByPrediction(rawResult1, kmeans.getFeaturesCol(), kmeans.getPredictionCol());
//        assertTrue(CollectionUtils.isEqualCollection(expectedGroups1, actualGroups1));
//
//        TestBlockingQueueManager.offerAll(trainId, trainData2);
//        TestBlockingQueueManager.poll(modelDataId);
//        TestBlockingQueueManager.offerAll(predictId, predictData);
//
//        List<Row> rawResult2 = TestBlockingQueueManager.poll(outputId, predictData.length);
//        List<Set<DenseVector>> actualGroups2 = groupFeaturesByPrediction(rawResult2, kmeans.getFeaturesCol(), kmeans.getPredictionCol());
//        assertTrue(CollectionUtils.isEqualCollection(expectedGroups1, actualGroups2));
//
//        client.cancel();
//    }
//
//    @Test
//    public void testHalfLife() throws Exception {
//        OnlineKMeans kmeans =
//                new OnlineKMeans()
//                        .setHalfLife(1, HasDecayFactor.POINT_UNIT)
//                        .setInitRandomCentroids(true)
//                        .setDims(2)
//                        .setBatchSize(6)
//                        .setFeaturesCol("features")
//                        .setPredictionCol("prediction");
//        KMeansModel model = kmeans.fit(trainTable);
//        Table outputTable = model.transform(predictTable)[0];
//
//        DataStream<Row> output = tEnv.toDataStream(outputTable);
//        output.addSink(new MockBlockingQueueSinkFunction<>(outputId));
//
//        DataStream<DenseVector[]> modelDataStream = model.getLatestModelData();
//        modelDataStream.addSink(new MockBlockingQueueSinkFunction<>(modelDataId));
//
//        JobClient client = env.executeAsync();
//
//        TestBlockingQueueManager.offerAll(trainId, trainData1);
//        TestBlockingQueueManager.poll(modelDataId);
//        TestBlockingQueueManager.offerAll(predictId, predictData);
//
//        List<Row> rawResult1 = TestBlockingQueueManager.poll(outputId, predictData.length);
//        List<Set<DenseVector>> actualGroups1 = groupFeaturesByPrediction(rawResult1, kmeans.getFeaturesCol(), kmeans.getPredictionCol());
//        assertTrue(CollectionUtils.isEqualCollection(expectedGroups1, actualGroups1));
//
//        TestBlockingQueueManager.offerAll(trainId, trainData2);
//        TestBlockingQueueManager.poll(modelDataId);
//        TestBlockingQueueManager.offerAll(predictId, predictData);
//
//        List<Row> rawResult2 = TestBlockingQueueManager.poll(outputId, predictData.length);
//        List<Set<DenseVector>> actualGroups2 = groupFeaturesByPrediction(rawResult2, kmeans.getFeaturesCol(), kmeans.getPredictionCol());
//        assertTrue(CollectionUtils.isEqualCollection(expectedGroups2, actualGroups2));
//
//        client.cancel();
//    }