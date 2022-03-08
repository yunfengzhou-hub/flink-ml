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

package org.apache.flink.ml.clustering;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.clustering.kmeans.KMeansModelData;
import org.apache.flink.ml.clustering.kmeans.StreamingKMeans;
import org.apache.flink.ml.clustering.kmeans.StreamingKMeansModel;
import org.apache.flink.ml.common.distance.EuclideanDistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.util.MockKVStore;
import org.apache.flink.ml.util.MockMessageQueues;
import org.apache.flink.ml.util.MockSinkFunction;
import org.apache.flink.ml.util.MockSourceFunction;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.ml.util.StageTestUtils;
import org.apache.flink.ml.util.TestMetricReporter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.commons.collections.CollectionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.ml.clustering.KMeansTest.groupFeaturesByPrediction;

/** Tests {@link StreamingKMeans} and {@link StreamingKMeansModel}. */
public class StreamingKMeansTest {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    private static final DenseVector[] trainData1 =
            new DenseVector[] {
                Vectors.dense(10.0, 0.0),
                Vectors.dense(10.0, 0.3),
                Vectors.dense(10.3, 0.0),
                Vectors.dense(-10.0, 0.0),
                Vectors.dense(-10.0, 0.6),
                Vectors.dense(-10.6, 0.0)
            };
    private static final DenseVector[] trainData2 =
            new DenseVector[] {
                Vectors.dense(10.0, 100.0),
                Vectors.dense(10.0, 100.3),
                Vectors.dense(10.3, 100.0),
                Vectors.dense(-10.0, -100.0),
                Vectors.dense(-10.0, -100.6),
                Vectors.dense(-10.6, -100.0)
            };
    private static final DenseVector[] predictData =
            new DenseVector[] {
                Vectors.dense(10.0, 10.0),
                Vectors.dense(10.3, 10.0),
                Vectors.dense(10.0, 10.3),
                Vectors.dense(-10.0, 10.0),
                Vectors.dense(-10.3, 10.0),
                Vectors.dense(-10.0, 10.3)
            };
    private static final List<Set<DenseVector>> expectedGroups1 =
            Arrays.asList(
                    new HashSet<>(
                            Arrays.asList(
                                    Vectors.dense(10.0, 10.0),
                                    Vectors.dense(10.3, 10.0),
                                    Vectors.dense(10.0, 10.3))),
                    new HashSet<>(
                            Arrays.asList(
                                    Vectors.dense(-10.0, 10.0),
                                    Vectors.dense(-10.3, 10.0),
                                    Vectors.dense(-10.0, 10.3))));
    private static final List<Set<DenseVector>> expectedGroups2 =
            Collections.singletonList(
                    new HashSet<>(
                            Arrays.asList(
                                    Vectors.dense(10.0, 10.0),
                                    Vectors.dense(10.3, 10.0),
                                    Vectors.dense(10.0, 10.3),
                                    Vectors.dense(-10.0, 10.0),
                                    Vectors.dense(-10.3, 10.0),
                                    Vectors.dense(-10.0, 10.3))));

    private String trainId;
    private String predictId;
    private String outputId;
    private String modelDataId;
    private String metricReporterPrefix;
    private String currentModelDataVersion;
    private List<String> queueIds;
    private List<String> kvStoreKeys;
    private List<JobClient> clients;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private Table offlineTrainTable;
    private Table trainTable;
    private Table predictTable;

    @Before
    public void before() {
        metricReporterPrefix = MockKVStore.createNonDuplicatePrefix();
        kvStoreKeys = new ArrayList<>();
        kvStoreKeys.add(metricReporterPrefix);

        currentModelDataVersion = "0";

        trainId = MockMessageQueues.createMessageQueue();
        predictId = MockMessageQueues.createMessageQueue();
        outputId = MockMessageQueues.createMessageQueue();
        modelDataId = MockMessageQueues.createMessageQueue();
        queueIds = new ArrayList<>();
        queueIds.addAll(Arrays.asList(trainId, predictId, outputId, modelDataId));

        clients = new ArrayList<>();

        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        config.setString("metrics.reporters", "test_reporter");
        config.setString(
                "metrics.reporter.test_reporter.class", TestMetricReporter.class.getName());
        config.setString("metrics.reporter.test_reporter.interval", "100 MILLISECONDS");
        config.setString("metrics.reporter.test_reporter.prefix", metricReporterPrefix);

        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(4);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);

        Schema schema = Schema.newBuilder().column("f0", DataTypes.of(DenseVector.class)).build();

        offlineTrainTable =
                tEnv.fromDataStream(env.fromElements(trainData1), schema).as("features");
        trainTable =
                tEnv.fromDataStream(
                                env.addSource(
                                        new MockSourceFunction<>(trainId),
                                        DenseVectorTypeInfo.INSTANCE),
                                schema)
                        .as("features");
        predictTable =
                tEnv.fromDataStream(
                                env.addSource(
                                        new MockSourceFunction<>(predictId),
                                        DenseVectorTypeInfo.INSTANCE),
                                schema)
                        .as("features");
    }

    /** Adds sinks for StreamingKMeansModel's transform output and model data. */
    private void configModelSink(StreamingKMeansModel streamingModel) {
        Table outputTable = streamingModel.transform(predictTable)[0];
        DataStream<Row> output = tEnv.toDataStream(outputTable);
        output.addSink(new MockSinkFunction<>(outputId));

        Table modelDataTable = streamingModel.getModelData()[0];
        DataStream<KMeansModelData> modelDataStream =
                KMeansModelData.getModelDataStream(modelDataTable);
        modelDataStream.addSink(new MockSinkFunction<>(modelDataId));
    }

    /** Blocks the thread until Model has set up init model data. */
    private void waitInitModelDataSetup() {
        while (!MockKVStore.containsKey(
                TestMetricReporter.getKey(metricReporterPrefix, "modelDataVersion"))) {
            Thread.yield();
        }
        waitModelDataUpdate();
    }

    /** Blocks the thread until the Model has received the next model-data-update event. */
    private void waitModelDataUpdate() {
        do {
            String tmpModelDataVersion =
                    MockKVStore.get(
                            TestMetricReporter.getKey(metricReporterPrefix, "modelDataVersion"));
            if (tmpModelDataVersion.equals(currentModelDataVersion)) {
                Thread.yield();
            } else {
                currentModelDataVersion = tmpModelDataVersion;
                break;
            }
        } while (true);
    }

    /**
     * Inserts default predict data to the predict queue, fetches the prediction results, and
     * asserts that the grouping result is as expected.
     *
     * @param expectedGroups A list containing sets of features, which is the expected group result
     * @param featuresCol Name of the column in the table that contains the features
     * @param predictionCol Name of the column in the table that contains the prediction result
     */
    private void predictAndAssert(
            List<Set<DenseVector>> expectedGroups, String featuresCol, String predictionCol)
            throws Exception {
        MockMessageQueues.offerAll(predictId, StreamingKMeansTest.predictData);
        List<Row> rawResult2 =
                MockMessageQueues.poll(outputId, StreamingKMeansTest.predictData.length);
        List<Set<DenseVector>> actualGroups2 =
                groupFeaturesByPrediction(rawResult2, featuresCol, predictionCol);
        Assert.assertTrue(CollectionUtils.isEqualCollection(expectedGroups, actualGroups2));
    }

    @Test
    public void testParam() {
        StreamingKMeans streamingKMeans = new StreamingKMeans();
        Assert.assertEquals("features", streamingKMeans.getFeaturesCol());
        Assert.assertEquals("prediction", streamingKMeans.getPredictionCol());
        Assert.assertEquals(EuclideanDistanceMeasure.NAME, streamingKMeans.getDistanceMeasure());
        Assert.assertEquals("random", streamingKMeans.getInitMode());
        Assert.assertEquals(2, streamingKMeans.getK());
        Assert.assertEquals(1, streamingKMeans.getDims());
        Assert.assertEquals("count", streamingKMeans.getBatchStrategy());
        Assert.assertEquals(1, streamingKMeans.getBatchSize());
        Assert.assertEquals(0., streamingKMeans.getDecayFactor(), 1e-5);
        Assert.assertEquals("random", streamingKMeans.getInitMode());
        Assert.assertEquals(StreamingKMeans.class.getName().hashCode(), streamingKMeans.getSeed());

        streamingKMeans
                .setK(9)
                .setFeaturesCol("test_feature")
                .setPredictionCol("test_prediction")
                .setK(3)
                .setDims(5)
                .setBatchSize(5)
                .setDecayFactor(0.25)
                .setInitMode("direct")
                .setSeed(100);

        Assert.assertEquals("test_feature", streamingKMeans.getFeaturesCol());
        Assert.assertEquals("test_prediction", streamingKMeans.getPredictionCol());
        Assert.assertEquals(3, streamingKMeans.getK());
        Assert.assertEquals(5, streamingKMeans.getDims());
        Assert.assertEquals("count", streamingKMeans.getBatchStrategy());
        Assert.assertEquals(5, streamingKMeans.getBatchSize());
        Assert.assertEquals(0.25, streamingKMeans.getDecayFactor(), 1e-5);
        Assert.assertEquals("direct", streamingKMeans.getInitMode());
        Assert.assertEquals(100, streamingKMeans.getSeed());
    }

    @Test
    public void testFitAndPredict() throws Exception {
        StreamingKMeans streamingKMeans =
                new StreamingKMeans()
                        .setInitMode("random")
                        .setDims(2)
                        .setInitWeights(new Double[] {0., 0.})
                        .setBatchSize(6)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        StreamingKMeansModel streamingModel = streamingKMeans.fit(trainTable);
        configModelSink(streamingModel);

        clients.add(env.executeAsync());
        waitInitModelDataSetup();

        MockMessageQueues.offerAll(trainId, trainData1);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups1,
                streamingKMeans.getFeaturesCol(),
                streamingKMeans.getPredictionCol());

        MockMessageQueues.offerAll(trainId, trainData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2,
                streamingKMeans.getFeaturesCol(),
                streamingKMeans.getPredictionCol());
    }

    @Test
    public void testInitWithOfflineKMeans() throws Exception {
        KMeans offlineKMeans =
                new KMeans().setFeaturesCol("features").setPredictionCol("prediction");
        KMeansModel offlineModel = offlineKMeans.fit(offlineTrainTable);

        StreamingKMeans streamingKMeans =
                new StreamingKMeans(offlineModel.getModelData())
                        .setInitMode("direct")
                        .setDims(2)
                        .setInitWeights(new Double[] {0., 0.})
                        .setBatchSize(6);
        ReadWriteUtils.updateExistingParams(streamingKMeans, offlineKMeans.getParamMap());

        StreamingKMeansModel streamingModel = streamingKMeans.fit(trainTable);
        configModelSink(streamingModel);

        clients.add(env.executeAsync());
        waitInitModelDataSetup();
        predictAndAssert(
                expectedGroups1,
                streamingKMeans.getFeaturesCol(),
                streamingKMeans.getPredictionCol());

        MockMessageQueues.offerAll(trainId, trainData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2,
                streamingKMeans.getFeaturesCol(),
                streamingKMeans.getPredictionCol());
    }

    @Test
    public void testDecayFactor() throws Exception {
        StreamingKMeans streamingKMeans =
                new StreamingKMeans()
                        .setInitMode("random")
                        .setDims(2)
                        .setInitWeights(new Double[] {0., 0.})
                        .setDecayFactor(100.0)
                        .setBatchSize(6)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        StreamingKMeansModel streamingModel = streamingKMeans.fit(trainTable);
        configModelSink(streamingModel);

        clients.add(env.executeAsync());
        waitInitModelDataSetup();

        MockMessageQueues.offerAll(trainId, trainData1);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups1,
                streamingKMeans.getFeaturesCol(),
                streamingKMeans.getPredictionCol());

        MockMessageQueues.offerAll(trainId, trainData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups1,
                streamingKMeans.getFeaturesCol(),
                streamingKMeans.getPredictionCol());
    }

    @Test
    public void testSaveAndReload() throws Exception {
        KMeans offlineKMeans =
                new KMeans().setFeaturesCol("features").setPredictionCol("prediction");
        KMeansModel offlineModel = offlineKMeans.fit(offlineTrainTable);

        StreamingKMeans streamingKMeans =
                new StreamingKMeans(offlineModel.getModelData())
                        .setDims(2)
                        .setBatchSize(6)
                        .setInitWeights(new Double[] {0., 0.});
        ReadWriteUtils.updateExistingParams(streamingKMeans, offlineKMeans.getParamMap());

        StreamingKMeans loadedKMeans =
                StageTestUtils.saveAndReload(
                        env, streamingKMeans, tempFolder.newFolder().getAbsolutePath());

        StreamingKMeansModel streamingModel = loadedKMeans.fit(trainTable);

        String modelDataPassId = MockMessageQueues.createMessageQueue();
        queueIds.add(modelDataPassId);

        String modelSavePath = tempFolder.newFolder().getAbsolutePath();
        streamingModel.save(modelSavePath);
        KMeansModelData.getModelDataStream(streamingModel.getModelData()[0])
                .addSink(new MockSinkFunction<>(modelDataPassId));
        clients.add(env.executeAsync());

        StreamingKMeansModel loadedModel = StreamingKMeansModel.load(env, modelSavePath);
        DataStream<KMeansModelData> loadedModelData =
                env.addSource(
                        new MockSourceFunction<>(modelDataPassId),
                        TypeInformation.of(KMeansModelData.class));
        loadedModel.setModelData(tEnv.fromDataStream(loadedModelData));

        configModelSink(loadedModel);

        clients.add(env.executeAsync());
        waitInitModelDataSetup();

        predictAndAssert(
                expectedGroups1,
                streamingKMeans.getFeaturesCol(),
                streamingKMeans.getPredictionCol());

        MockMessageQueues.offerAll(trainId, trainData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2,
                streamingKMeans.getFeaturesCol(),
                streamingKMeans.getPredictionCol());
    }

    @Test
    public void testGetModelData() throws Exception {
        StreamingKMeans streamingKMeans =
                new StreamingKMeans()
                        .setInitMode("random")
                        .setDims(2)
                        .setInitWeights(new Double[] {0., 0.})
                        .setBatchSize(6)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        StreamingKMeansModel streamingModel = streamingKMeans.fit(trainTable);
        configModelSink(streamingModel);

        clients.add(env.executeAsync());
        MockMessageQueues.poll(modelDataId);

        MockMessageQueues.offerAll(trainId, trainData1);
        KMeansModelData actualModelData = MockMessageQueues.poll(modelDataId);

        KMeansModelData expectedModelData =
                new KMeansModelData(
                        new DenseVector[] {Vectors.dense(10.1, 0.1), Vectors.dense(-10.2, 0.2)});

        Assert.assertEquals(expectedModelData.centroids.length, actualModelData.centroids.length);
        for (int i = 0; i < expectedModelData.centroids.length; i++) {
            Assert.assertArrayEquals(
                    expectedModelData.centroids[i].values,
                    actualModelData.centroids[i].values,
                    1e-5);
        }
    }

    @Test
    public void testSetModelData() throws Exception {
        KMeansModelData modelData1 =
                new KMeansModelData(
                        new DenseVector[] {Vectors.dense(10.1, 0.1), Vectors.dense(-10.2, 0.2)});

        KMeansModelData modelData2 =
                new KMeansModelData(
                        new DenseVector[] {
                            Vectors.dense(10.1, 100.1), Vectors.dense(-10.2, -100.2)
                        });

        String modelDataInputId = MockMessageQueues.createMessageQueue();
        queueIds.add(modelDataInputId);
        Table modelDataTable =
                tEnv.fromDataStream(
                        env.addSource(
                                new MockSourceFunction<>(modelDataInputId),
                                TypeInformation.of(KMeansModelData.class)));

        StreamingKMeansModel streamingModel =
                new StreamingKMeansModel()
                        .setModelData(modelDataTable)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        configModelSink(streamingModel);

        clients.add(env.executeAsync());

        MockMessageQueues.offerAll(modelDataInputId, modelData1);
        waitInitModelDataSetup();
        predictAndAssert(
                expectedGroups1,
                streamingModel.getFeaturesCol(),
                streamingModel.getPredictionCol());

        MockMessageQueues.offerAll(modelDataInputId, modelData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2,
                streamingModel.getFeaturesCol(),
                streamingModel.getPredictionCol());
    }

    @After
    public void after() {
        for (JobClient client : clients) {
            try {
                client.cancel();
            } catch (IllegalStateException e) {
                if (!e.getMessage()
                        .equals("MiniCluster is not yet running or has already been shut down.")) {
                    throw e;
                }
            }
        }
        clients.clear();

        for (String queueId : queueIds) {
            MockMessageQueues.deleteBlockingQueue(queueId);
        }
        queueIds.clear();

        for (String key : kvStoreKeys) {
            MockKVStore.remove(key);
        }
        kvStoreKeys.clear();
    }
}
