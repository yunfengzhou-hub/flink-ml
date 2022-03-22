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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.apache.commons.collections.CollectionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.ml.clustering.KMeansTest.groupFeaturesByPrediction;

/** Tests {@link OnlineKMeans} and {@link OnlineKMeansModel}. */
public class OnlineKMeansTest extends AbstractTestBase {
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

    private String currentModelDataVersion;

    private InMemorySourceFunction<DenseVector> trainSource;
    private InMemorySourceFunction<DenseVector> predictSource;
    private InMemorySinkFunction<Row> outputSink;
    private InMemorySinkFunction<KMeansModelData> modelDataSink;

    private InMemoryReporter reporter;
    private MiniCluster miniCluster;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    private Table offlineTrainTable;
    private Table trainTable;
    private Table predictTable;

    @Before
    public void before() throws Exception {
        currentModelDataVersion = "0";

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

        Schema schema = Schema.newBuilder().column("f0", DataTypes.of(DenseVector.class)).build();

        offlineTrainTable =
                tEnv.fromDataStream(env.fromElements(trainData1), schema).as("features");
        trainTable =
                tEnv.fromDataStream(
                                env.addSource(trainSource, DenseVectorTypeInfo.INSTANCE), schema)
                        .as("features");
        predictTable =
                tEnv.fromDataStream(
                                env.addSource(predictSource, DenseVectorTypeInfo.INSTANCE), schema)
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
    private void configTransformAndSink(OnlineKMeansModel onlineModel) {
        Table outputTable = onlineModel.transform(predictTable)[0];
        DataStream<Row> output = tEnv.toDataStream(outputTable);
        output.addSink(outputSink);

        Table modelDataTable = onlineModel.getModelData()[0];
        DataStream<KMeansModelData> modelDataStream =
                KMeansModelData.getModelDataStream(modelDataTable);
        modelDataStream.addSink(modelDataSink);
    }

    /** Blocks the thread until Model has set up init model data. */
    private void waitInitModelDataSetup() {
        while (reporter.findMetrics("modelDataVersion").size() < defaultParallelism) {
            Thread.yield();
        }
        waitModelDataUpdate();
    }

    /** Blocks the thread until the Model has received the next model-data-update event. */
    @SuppressWarnings("unchecked")
    private void waitModelDataUpdate() {
        do {
            String tmpModelDataVersion =
                    String.valueOf(
                            reporter.findMetrics("modelDataVersion").values().stream()
                                    .map(x -> Integer.parseInt(((Gauge<String>) x).getValue()))
                                    .min(Integer::compareTo)
                                    .orElse(Integer.parseInt(currentModelDataVersion)));
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
        predictSource.offerAll(OnlineKMeansTest.predictData);
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
        Assert.assertEquals(EuclideanDistanceMeasure.NAME, onlineKMeans.getDistanceMeasure());
        Assert.assertEquals("random", onlineKMeans.getInitMode());
        Assert.assertEquals(2, onlineKMeans.getK());
        Assert.assertEquals(1, onlineKMeans.getDims());
        Assert.assertEquals("count", onlineKMeans.getBatchStrategy());
        Assert.assertEquals(1, onlineKMeans.getBatchSize());
        Assert.assertEquals(0., onlineKMeans.getDecayFactor(), 1e-5);
        Assert.assertEquals("random", onlineKMeans.getInitMode());
        Assert.assertEquals(OnlineKMeans.class.getName().hashCode(), onlineKMeans.getSeed());

        onlineKMeans
                .setK(9)
                .setFeaturesCol("test_feature")
                .setPredictionCol("test_prediction")
                .setK(3)
                .setDims(5)
                .setBatchSize(5)
                .setDecayFactor(0.25)
                .setInitMode("direct")
                .setSeed(100);

        Assert.assertEquals("test_feature", onlineKMeans.getFeaturesCol());
        Assert.assertEquals("test_prediction", onlineKMeans.getPredictionCol());
        Assert.assertEquals(3, onlineKMeans.getK());
        Assert.assertEquals(5, onlineKMeans.getDims());
        Assert.assertEquals("count", onlineKMeans.getBatchStrategy());
        Assert.assertEquals(5, onlineKMeans.getBatchSize());
        Assert.assertEquals(0.25, onlineKMeans.getDecayFactor(), 1e-5);
        Assert.assertEquals("direct", onlineKMeans.getInitMode());
        Assert.assertEquals(100, onlineKMeans.getSeed());
    }

    @Test
    public void testFitAndPredict() throws Exception {
        OnlineKMeans onlineKMeans =
                new OnlineKMeans()
                        .setInitMode("random")
                        .setDims(2)
                        .setInitWeights(new Double[] {0., 0.})
                        .setBatchSize(6)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        OnlineKMeansModel onlineModel = onlineKMeans.fit(trainTable);
        configTransformAndSink(onlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());
        waitInitModelDataSetup();

        trainSource.offerAll(trainData1);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups1, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());

        trainSource.offerAll(trainData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());
    }

    @Test
    public void testInitWithKMeans() throws Exception {
        KMeans kMeans = new KMeans().setFeaturesCol("features").setPredictionCol("prediction");
        KMeansModel kMeansModel = kMeans.fit(offlineTrainTable);

        OnlineKMeans onlineKMeans =
                new OnlineKMeans(kMeansModel.getModelData())
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction")
                        .setInitMode("direct")
                        .setDims(2)
                        .setInitWeights(new Double[] {0., 0.})
                        .setBatchSize(6);

        OnlineKMeansModel onlineModel = onlineKMeans.fit(trainTable);
        configTransformAndSink(onlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());
        waitInitModelDataSetup();
        predictAndAssert(
                expectedGroups1, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());

        trainSource.offerAll(trainData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());
    }

    @Test
    public void testDecayFactor() throws Exception {
        KMeans kMeans = new KMeans().setFeaturesCol("features").setPredictionCol("prediction");
        KMeansModel kMeansModel = kMeans.fit(offlineTrainTable);

        OnlineKMeans onlineKMeans =
                new OnlineKMeans(kMeansModel.getModelData())
                        .setDims(2)
                        .setInitWeights(new Double[] {3., 3.})
                        .setDecayFactor(0.5)
                        .setBatchSize(6)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        OnlineKMeansModel onlineModel = onlineKMeans.fit(trainTable);
        configTransformAndSink(onlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());
        modelDataSink.poll();

        trainSource.offerAll(trainData2);
        KMeansModelData actualModelData = modelDataSink.poll();

        KMeansModelData expectedModelData =
                new KMeansModelData(
                        new DenseVector[] {
                            Vectors.dense(10.1, 200.3 / 3), Vectors.dense(-10.2, -200.2 / 3)
                        });

        Assert.assertEquals(expectedModelData.centroids.length, actualModelData.centroids.length);
        Arrays.sort(actualModelData.centroids, (o1, o2) -> (int) (o2.values[0] - o1.values[0]));
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
        KMeansModel kMeansModel = kMeans.fit(offlineTrainTable);

        OnlineKMeans onlineKMeans =
                new OnlineKMeans(kMeansModel.getModelData())
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction")
                        .setInitMode("direct")
                        .setDims(2)
                        .setInitWeights(new Double[] {0., 0.})
                        .setBatchSize(6);

        String savePath = tempFolder.newFolder().getAbsolutePath();
        onlineKMeans.save(savePath);
        miniCluster.executeJobBlocking(env.getStreamGraph().getJobGraph());
        OnlineKMeans loadedKMeans = OnlineKMeans.load(env, savePath);

        OnlineKMeansModel onlineModel = loadedKMeans.fit(trainTable);

        String modelSavePath = tempFolder.newFolder().getAbsolutePath();
        onlineModel.save(modelSavePath);
        OnlineKMeansModel loadedModel = OnlineKMeansModel.load(env, modelSavePath);
        loadedModel.setModelData(onlineModel.getModelData());

        configTransformAndSink(loadedModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());
        waitInitModelDataSetup();
        predictAndAssert(
                expectedGroups1, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());

        trainSource.offerAll(trainData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2, onlineKMeans.getFeaturesCol(), onlineKMeans.getPredictionCol());
    }

    @Test
    public void testGetModelData() throws Exception {
        OnlineKMeans onlineKMeans =
                new OnlineKMeans()
                        .setInitMode("random")
                        .setDims(2)
                        .setInitWeights(new Double[] {0., 0.})
                        .setBatchSize(6)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        OnlineKMeansModel onlineModel = onlineKMeans.fit(trainTable);
        configTransformAndSink(onlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());
        modelDataSink.poll();

        trainSource.offerAll(trainData1);
        KMeansModelData actualModelData = modelDataSink.poll();

        KMeansModelData expectedModelData =
                new KMeansModelData(
                        new DenseVector[] {Vectors.dense(10.1, 0.1), Vectors.dense(-10.2, 0.2)});

        Assert.assertEquals(expectedModelData.centroids.length, actualModelData.centroids.length);
        Arrays.sort(actualModelData.centroids, (o1, o2) -> (int) (o2.values[0] - o1.values[0]));
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

        InMemorySourceFunction<KMeansModelData> modelDataSource = new InMemorySourceFunction<>();
        Table modelDataTable =
                tEnv.fromDataStream(
                        env.addSource(modelDataSource, TypeInformation.of(KMeansModelData.class)));

        OnlineKMeansModel onlineModel =
                new OnlineKMeansModel()
                        .setModelData(modelDataTable)
                        .setFeaturesCol("features")
                        .setPredictionCol("prediction");
        configTransformAndSink(onlineModel);

        miniCluster.submitJob(env.getStreamGraph().getJobGraph());

        modelDataSource.offerAll(modelData1);
        waitInitModelDataSetup();
        predictAndAssert(
                expectedGroups1, onlineModel.getFeaturesCol(), onlineModel.getPredictionCol());

        modelDataSource.offerAll(modelData2);
        waitModelDataUpdate();
        predictAndAssert(
                expectedGroups2, onlineModel.getFeaturesCol(), onlineModel.getPredictionCol());
    }
}
