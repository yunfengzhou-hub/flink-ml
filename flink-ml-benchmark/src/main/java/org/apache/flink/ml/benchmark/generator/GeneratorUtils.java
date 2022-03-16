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

package org.apache.flink.ml.benchmark.generator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Random;

/** Utility methods to generate data for benchmarks. */
public class GeneratorUtils {
    /**
     * Generates random continuous vectors.
     *
     * @param tEnv The streaming table execution environment instance.
     * @param numExamples Number of examples to generate in total.
     * @param seed The seed to generate seed on each partition.
     * @param dims Dimension of the vectors to be generated.
     * @return The generated vector stream.
     */
    public static Table generateRandomContinuousVectorStream(
            StreamTableEnvironment tEnv, long numExamples, long seed, int dims) {
        StreamExecutionEnvironment env = ((StreamTableEnvironmentImpl) tEnv).execEnv();

        DataStream<Vector> stream =
                env.fromParallelCollection(
                                new NumberSequenceIterator(1L, numExamples),
                                BasicTypeInfo.LONG_TYPE_INFO)
                        .map(new GenerateRandomContinuousVectorFunction(seed, dims));

        Schema schema = Schema.newBuilder().column("f0", DataTypes.of(DenseVector.class)).build();

        return tEnv.fromDataStream(stream, schema);
    }

    private static class GenerateRandomContinuousVectorFunction
            extends RichMapFunction<Long, Vector> {
        private final int dims;
        private final long initSeed;
        private Random random;

        private GenerateRandomContinuousVectorFunction(long initSeed, int dims) {
            this.dims = dims;
            this.initSeed = initSeed;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            int index = getRuntimeContext().getIndexOfThisSubtask();
            random = new Random(Tuple2.of(initSeed, index).hashCode());
        }

        @Override
        public Vector map(Long value) {
            double[] values = new double[dims];
            for (int i = 0; i < dims; i++) {
                values[i] = random.nextDouble();
            }
            return Vectors.dense(values);
        }
    }
}
