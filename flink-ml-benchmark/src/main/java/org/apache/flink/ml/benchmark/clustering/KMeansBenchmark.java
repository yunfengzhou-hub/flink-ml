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

package org.apache.flink.ml.benchmark.clustering;

import org.apache.flink.ml.benchmark.DataGenerator;
import org.apache.flink.ml.benchmark.EstimatorBenchmark;
import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.clustering.kmeans.KMeansParams;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Benchmark of {@link KMeans}. */
public class KMeansBenchmark extends EstimatorBenchmark<KMeans, KMeansModel, KMeansBenchmark>
        implements KMeansParams<KMeansBenchmark> {

    public KMeansBenchmark() {
        super(KMeans.class);
    }

    @Override
    public Table[] getTrainData(StreamTableEnvironment tEnv) {
        Table dataTable =
                DataGenerator.generateRandomContinuousVectorStream(
                        tEnv, getDataSize(), getSeed(), getDims());
        return new Table[] {dataTable.as(getFeaturesCol())};
    }
}
