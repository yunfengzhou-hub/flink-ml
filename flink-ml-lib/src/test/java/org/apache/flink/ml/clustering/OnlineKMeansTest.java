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
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.clustering.kmeans.KMeansModelData;
import org.apache.flink.ml.clustering.kmeans.KMeansUtils;
import org.apache.flink.ml.clustering.kmeans.OnlineKMeans;
import org.apache.flink.ml.clustering.kmeans.OnlineKMeansModel;
import org.apache.flink.ml.common.distance.EuclideanDistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.util.InMemorySinkFunction;
import org.apache.flink.ml.util.InMemorySourceFunction;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.apache.commons.collections.CollectionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.ml.clustering.KMeansTest.groupFeaturesByPrediction;

/** Tests {@link OnlineKMeans} and {@link OnlineKMeansModel}. */
public class OnlineKMeansTest extends TestLogger {
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

    private static final int defaultParallelism = 4;
    private static final int numTaskManagers = 2;
    private static final int numSlotsPerTaskManager = 2;

    private int currentModelDataVersion;

    private InMemorySourceFunction<DenseVector> trainSource;
    private InMemorySourceFunction<DenseVector> predictSource;
    private InMemorySinkFunction<Row> outputSink;
    private InMemorySinkFunction<KMeansModelData> modelDataSink;

    // TODO: creates static mini cluster once for whole test class after dependency upgrades to
    // Flink 1.15.
    private InMemoryReporter reporter;
    private MiniCluster miniCluster;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    private Table offlineTrainTable;
    private Table onlineTrainTable;
    private Table onlinePredictTable;

    @Before
    public void before() throws Exception {
        currentModelDataVersion = 0;

        trainSource = new InMemorySourceFunction<>();
        predictSource = new InMemorySourceFunction<>();
        outputSink = new InMemorySinkFunction<>();
        modelDataSink = new InMemorySinkFunction<>();

        Configuration config = new Configuration();
        config.set(RestOptions.BIND_PORT, "18081-19091");
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        reporter = InMemoryReporter.createWithRetainedMetrics();
        reporter.addToConfiguration(config);

        miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumTaskManagers(numTaskManagers)
                                .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                                .build());
        miniCluster.start();

        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(defaultParallelism);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);

        offlineTrainTable = tEnv.fromDataStream(env.fromElements(trainData1)).as("features");
        onlineTrainTable =
                tEnv.fromDataStream(env.addSource(trainSource, DenseVectorTypeInfo.INSTANCE))
                        .as("features");
        onlinePredictTable =
                tEnv.fromDataStream(env.addSource(predictSource, DenseVectorTypeInfo.INSTANCE))
                        .as("features");
    }

    @After
    public void after() throws Exception {
        miniCluster.close();
    }

    /**
     * Performs transform() on the provided model with predictTable, and adds sinks for
     * OnlineKMeansModel's transform output and model data.
     */
    private void transformAndOutputData(OnlineKMeansModel onlineModel) {
        Table outputTable = onlineModel.transform(onlinePredictTable)[0];
        tEnv.toDataStream(outputTable).addSink(outputSink);

        Table modelDataTable = onlineModel.getModelData()[0];
        KMeansModelData.getModelDataStream(modelDataTable).addSink(modelDataSink);
    }

    /** Blocks the thread until Model has set up init model data. */
    private void waitInitModelDataSetup() throws InterruptedException {
        while (reporter.findMetrics(OnlineKMeansModel.MODEL_DATA_VERSION_GAUGE_KEY).size()
                < defaultParallelism) {
            Thread.sleep(100);
        }
        waitModelDataUpdate();
    }

    /** Blocks the thread until the Model has received the next model-data-update event. */
    @SuppressWarnings("unchecked")
    private void waitModelDataUpdate() throws InterruptedException {
        do {
            int tmpModelDataVersion =
                    reporter.findMetrics(OnlineKMeansModel.MODEL_DATA_VERSION_GAUGE_KEY).values()
                            .stream()
                            .map(x -> Integer.parseInt(((Gauge<String>) x).getValue()))
                            .min(Integer::compareTo)
                            .get();
            if (tmpModelDataVersion == currentModelDataVersion) {
                Thread.sleep(100);
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
        predictSource.addAll(OnlineKMeansTest.predictData);
        List<Row> rawResult = outputSink.poll(OnlineKMeansTest.predictData.length);
        List<Set<DenseVector>> actualGroups =
                groupFeaturesByPrediction(rawResult, featuresCol, predictionCol);
        Assert.assertTrue(CollectionUtils.isEqualCollection(expectedGroups, actualGroups));
    }

    @Test
    public void testParam() {
        OnlineKMeans onlineKMeans = new OnlineKMeans();
        Assert.assertEquals("features", onlineKMeans.getFeaturesCol());
        Assert.assertEquals("prediction", onlineKMeans.getPredictionCol());
        Assert.assertEquals("count", onlineKMeans.getBatchStrategy());
        Assert.assertEquals(EuclideanDistanceMeasure.NAME, onlineKMeans.getDistanceMeasure());
        Assert.assertEquals(32, onlineKMeans.getGlobalBatchSize());
        Assert.assertEquals(0., onlineKMeans.getDecayFactor(), 1e-5);
        Assert.assertEquals(OnlineKMeans.class.getName().hashCode(), onlineKMeans.getSeed());

        onlineKMeans
                .setFeaturesCol("test_feature")
                .setPredictionCol("test_prediction")
                .setGlobalBatchSize(5)
                .setDecayFactor(0.25)
                .setSeed(100);

        Assert.assertEquals("test_feature", onlineKMeans.getFeaturesCol());
        Assert.assertEquals("test_prediction", onlineKMeans.getPredictionCol());
        Assert.assertEquals("count", onlineKMeans.getBatchStrategy());
        Assert.assertEquals(EuclideanDistanceMeasure.NAME, onlineKMeans.getDistanceMeasure());
        Assert.assertEquals(5, onlineKMeans.getGlobalBatchSize());
        Assert.assertEquals(0.25, onlineKMeans.getDecayFactor(), 1e-5);
        Assert.assertEquals(100, onlineKMeans.getSeed());
    }

    @Test
    public void testFitAndPredict() throws Exception {
        OnlineKMeans onlineKMeans =
                new OnlineKMeans()
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction")
                        .setGlobalBatchSize(6)
                        .setInitialModelData(
                                KMeansUtils.generateRandomModelData(env, 2, 2, 0.0, 0));
        OnlineKMeansModel onlineModel = onlineKMeans.fit(onlineTrainTable);
        transformAndOutputData(onlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());
        waitInitModelDataSetup();

        trainSource.addAll(trainData1);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups1, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());

        trainSource.addAll(trainData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());
    }

    @Test
    public void testInitWithKMeans() throws Exception {
        KMeans kMeans = new KMeans().setFeaturesCol("features").setPredictionCol("prediction");
        KMeansModel model = kMeans.fit(offlineTrainTable);

        OnlineKMeans onlineKMeans =
                new OnlineKMeans()
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction")
                        .setGlobalBatchSize(6)
                        .setInitialModelData(model.getModelData()[0]);

        OnlineKMeansModel onlineModel = onlineKMeans.fit(onlineTrainTable);
        transformAndOutputData(onlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());
        waitInitModelDataSetup();
        predictAndAssert(
                expectedGroups1, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());

        trainSource.addAll(trainData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());
    }

    @Test
    public void testDecayFactor() throws Exception {
        KMeans kMeans = new KMeans().setFeaturesCol("features").setPredictionCol("prediction");
        KMeansModel model = kMeans.fit(offlineTrainTable);

        OnlineKMeans onlineKMeans =
                new OnlineKMeans()
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction")
                        .setGlobalBatchSize(6)
                        .setDecayFactor(0.5)
                        .setInitialModelData(model.getModelData()[0]);
        OnlineKMeansModel onlineModel = onlineKMeans.fit(onlineTrainTable);
        transformAndOutputData(onlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());
        modelDataSink.poll();

        trainSource.addAll(trainData2);
        KMeansModelData actualModelData = modelDataSink.poll();

        KMeansModelData expectedModelData =
                new KMeansModelData(
                        new DenseVector[] {
                            Vectors.dense(-10.2, -200.2 / 3), Vectors.dense(10.1, 200.3 / 3)
                        },
                        Vectors.dense(4.5, 4.5));

        Assert.assertArrayEquals(
                expectedModelData.weights.values, actualModelData.weights.values, 1e-5);
        Assert.assertEquals(expectedModelData.centroids.length, actualModelData.centroids.length);
        Arrays.sort(actualModelData.centroids, Comparator.comparingDouble(vector -> vector.get(0)));
        for (int i = 0; i < expectedModelData.centroids.length; i++) {
            Assert.assertArrayEquals(
                    expectedModelData.centroids[i].values,
                    actualModelData.centroids[i].values,
                    1e-5);
        }
    }

    @Test
    public void testSaveAndReload() throws Exception {
        KMeans kMeans = new KMeans().setFeaturesCol("features").setPredictionCol("prediction");
        KMeansModel model = kMeans.fit(offlineTrainTable);

        OnlineKMeans onlineKMeans =
                new OnlineKMeans()
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction")
                        .setGlobalBatchSize(6)
                        .setInitialModelData(model.getModelData()[0]);

        String savePath = tempFolder.newFolder().getAbsolutePath();
        onlineKMeans.save(savePath);
        miniCluster.executeJobBlocking(env.getStreamGraph().getJobGraph());
        OnlineKMeans loadedOnlineKMeans = OnlineKMeans.load(env, savePath);

        OnlineKMeansModel onlineModel = loadedOnlineKMeans.fit(onlineTrainTable);

        String modelSavePath = tempFolder.newFolder().getAbsolutePath();
        onlineModel.save(modelSavePath);
        OnlineKMeansModel loadedOnlineModel = OnlineKMeansModel.load(env, modelSavePath);
        loadedOnlineModel.setModelData(onlineModel.getModelData());

        transformAndOutputData(loadedOnlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());
        waitInitModelDataSetup();
        predictAndAssert(
                expectedGroups1, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());

        trainSource.addAll(trainData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());
    }

    @Test
    public void testGetModelData() throws Exception {
        OnlineKMeans onlineKMeans =
                new OnlineKMeans()
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction")
                        .setGlobalBatchSize(6)
                        .setInitialModelData(
                                KMeansUtils.generateRandomModelData(env, 2, 2, 0.0, 0));
        OnlineKMeansModel onlineModel = onlineKMeans.fit(onlineTrainTable);
        transformAndOutputData(onlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());
        modelDataSink.poll();

        trainSource.addAll(trainData1);
        KMeansModelData actualModelData = modelDataSink.poll();

        KMeansModelData expectedModelData =
                new KMeansModelData(
                        new DenseVector[] {Vectors.dense(-10.2, 0.2), Vectors.dense(10.1, 0.1)},
                        Vectors.dense(3, 3));

        Assert.assertArrayEquals(
                expectedModelData.weights.values, actualModelData.weights.values, 1e-5);
        Assert.assertEquals(expectedModelData.centroids.length, actualModelData.centroids.length);
        Arrays.sort(actualModelData.centroids, Comparator.comparingDouble(vector -> vector.get(0)));
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
                        new DenseVector[] {Vectors.dense(10.1, 0.1), Vectors.dense(-10.2, 0.2)},
                        Vectors.dense(0.0, 0.0));

        KMeansModelData modelData2 =
                new KMeansModelData(
                        new DenseVector[] {
                            Vectors.dense(10.1, 100.1), Vectors.dense(-10.2, -100.2)
                        },
                        Vectors.dense(0.0, 0.0));

        InMemorySourceFunction<KMeansModelData> modelDataSource = new InMemorySourceFunction<>();
        Table modelDataTable =
                tEnv.fromDataStream(
                        env.addSource(modelDataSource, TypeInformation.of(KMeansModelData.class)));

        OnlineKMeansModel onlineModel =
                new OnlineKMeansModel()
                        .setModelData(modelDataTable)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        transformAndOutputData(onlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());

        modelDataSource.addAll(modelData1);
        waitInitModelDataSetup();
        predictAndAssert(
                expectedGroups1, onlineModel.getFeaturesCol(), onlineModel.getPredictionCol());

        modelDataSource.addAll(modelData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2, onlineModel.getFeaturesCol(), onlineModel.getPredictionCol());
    }
}
