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

package org.apache.flink.ml.benchmark;

import org.apache.flink.ml.benchmark.data.InputDataGenerator;
import org.apache.flink.ml.benchmark.data.clustering.KMeansModelDataGenerator;
import org.apache.flink.ml.benchmark.data.common.DenseVectorGenerator;
import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.common.distance.EuclideanDistanceMeasure;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests benchmarks. */
@SuppressWarnings("unchecked")
public class BenchmarkTest extends AbstractTestBase {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    @SuppressWarnings({"unchecked"})
    public void testParseJsonFile() throws Exception {
        File configFile = new File(tempFolder.newFolder().getAbsolutePath() + "/test-conf.json");
        InputStream inputStream =
                this.getClass().getClassLoader().getResourceAsStream("benchmark-conf.json");
        FileUtils.copyInputStreamToFile(inputStream, configFile);

        Map<String, ?> benchmarks = BenchmarkUtils.parseJsonFile(configFile.getAbsolutePath());
        assertEquals(benchmarks.size(), 1);
        assertTrue(benchmarks.containsKey("KMeansModel-1"));

        Map<String, ?> benchmark = (Map<String, ?>) benchmarks.get("KMeansModel-1");

        KMeansModel stage =
                ReadWriteUtils.instantiateWithParams((Map<String, ?>) benchmark.get("stage"));
        assertEquals("prediction", stage.getPredictionCol());
        assertEquals("features", stage.getFeaturesCol());
        assertEquals(2, stage.getK());
        assertEquals(EuclideanDistanceMeasure.NAME, stage.getDistanceMeasure());

        DenseVectorGenerator inputDataGenerator =
                ReadWriteUtils.instantiateWithParams((Map<String, ?>) benchmark.get("inputData"));
        assertArrayEquals(new String[] {"features"}, inputDataGenerator.getColNames());
        assertEquals(2, inputDataGenerator.getSeed());
        assertEquals(10000, inputDataGenerator.getNumValues());
        assertEquals(10, inputDataGenerator.getVectorDim());

        KMeansModelDataGenerator modelDataGenerator =
                ReadWriteUtils.instantiateWithParams((Map<String, ?>) benchmark.get("modelData"));
        assertEquals(1, modelDataGenerator.getSeed());
        assertEquals(2, modelDataGenerator.getArraySize());
        assertEquals(10, modelDataGenerator.getVectorDim());
    }

    @Test
    public void testRunBenchmark() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        KMeans kMeans = new KMeans().setK(5).setFeaturesCol("test_feature");
        InputDataGenerator<?> inputsGenerator =
                new DenseVectorGenerator()
                        .setColNames("test_feature")
                        .setNumValues(1000L)
                        .setVectorDim(10);

        long estimatedTime = System.currentTimeMillis();
        BenchmarkResult result =
                BenchmarkUtils.runBenchmark(tEnv, "testBenchmarkName", kMeans, inputsGenerator);
        estimatedTime = System.currentTimeMillis() - estimatedTime;

        assertEquals("testBenchmarkName", result.name);
        assertTrue(result.totalTimeMs > 0);
        assertTrue(result.totalTimeMs <= estimatedTime);
        assertEquals(1000L, (long) result.inputRecordNum);
        assertEquals(1L, (long) result.outputRecordNum);
        assertEquals(
                result.inputRecordNum * 1000.0 / result.totalTimeMs, result.inputThroughput, 1e-5);
        assertEquals(
                result.outputRecordNum * 1000.0 / result.totalTimeMs,
                result.outputThroughput,
                1e-5);
    }
}
