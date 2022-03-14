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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.util.InstantiationUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** This class represents benchmark instances for {@link Estimator}s. */
public abstract class EstimatorBenchmark<
                E extends Estimator<E, M>,
                M extends Model<M>,
                T extends EstimatorBenchmark<E, M, T>>
        extends Benchmark<T> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private final Class<E> clazz;

    public EstimatorBenchmark(Class<E> clazz) {
        super();
        this.clazz = clazz;
    }

    /**
     * Generates Tables containing data to be used in the {@link Estimator#fit(Table...)} process.
     */
    public abstract Table[] getTrainData(StreamTableEnvironment tEnv);

    // Below is a simple example of the implementation of execute(). Does not represent final
    // implementation.
    @Override
    public BenchmarkResult execute(StreamTableEnvironment tEnv) throws Exception {
        Table[] trainData = getTrainData(tEnv);

        Estimator<E, M> estimator = InstantiationUtil.instantiate(clazz);
        ReadWriteUtils.updateExistingParams(estimator, paramMap);
        Model<M> model = estimator.fit(trainData);

        for (Table table : model.getModelData()) {
            tEnv.toDataStream(table).addSink(new DiscardingSink<>());
        }

        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();
        JobExecutionResult result = env.execute();

        return new BenchmarkResult(this)
                .setExecutionTime(result.getNetRuntime(TimeUnit.MILLISECONDS));
    }
}
