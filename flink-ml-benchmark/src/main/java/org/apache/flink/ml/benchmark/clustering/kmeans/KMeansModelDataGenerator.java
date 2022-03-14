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

package org.apache.flink.ml.benchmark.clustering.kmeans;

import org.apache.flink.ml.benchmark.generator.DataGenerator;
import org.apache.flink.ml.benchmark.generator.GeneratorParams;
import org.apache.flink.ml.benchmark.generator.GeneratorUtils;
import org.apache.flink.ml.clustering.kmeans.KMeansModelData;
import org.apache.flink.ml.clustering.kmeans.KMeansParams;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that generates table arrays containing model data for {@link
 * org.apache.flink.ml.clustering.kmeans.KMeansModel}.
 */
public class KMeansModelDataGenerator
        implements DataGenerator<KMeansModelDataGenerator>,
                GeneratorParams<KMeansModelDataGenerator>,
                KMeansParams<KMeansModelDataGenerator> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public KMeansModelDataGenerator() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] getData(StreamTableEnvironment tEnv) {
        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();
        DataStream<KMeansModelData> stream =
                GeneratorUtils.generateRandomContinuousVectorArrayStream(
                                env, getNumData(), getK(), getSeed(), getDims())
                        .map(KMeansModelData::new);
        Table table = tEnv.fromDataStream(stream);
        return new Table[] {table};
    }

    @Override
    public long getNumElements() {
        return 1;
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }
}
