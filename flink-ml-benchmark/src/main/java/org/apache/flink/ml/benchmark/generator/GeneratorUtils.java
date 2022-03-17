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
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Random;

/** Utility methods to generate data for benchmarks. */
public class GeneratorUtils {
    /**
     * Generates random continuous vectors.
     *
     * @param env The stream execution environment.
     * @param numData Number of examples to generate in total.
     * @param seed The seed to generate seed on each partition.
     * @param dims Dimension of the vectors to be generated.
     * @return The generated vector stream.
     */
    public static DataStream<DenseVector> generateRandomContinuousVectorStream(
            StreamExecutionEnvironment env, long numData, long seed, int dims) {
        return env.fromParallelCollection(
                        new NumberSequenceIterator(1L, numData), BasicTypeInfo.LONG_TYPE_INFO)
                .map(new GenerateRandomContinuousVectorFunction(seed, dims));
    }

    private static class GenerateRandomContinuousVectorFunction
            extends RichMapFunction<Long, DenseVector> {
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
        public DenseVector map(Long value) {
            double[] values = new double[dims];
            for (int i = 0; i < dims; i++) {
                values[i] = random.nextDouble();
            }
            return Vectors.dense(values);
        }
    }

    /**
     * Generates random continuous vector arrays.
     *
     * @param env The stream execution environment.
     * @param numData Number of examples to generate in total.
     * @param arraySize Size of the vector array.
     * @param seed The seed to generate seed on each partition.
     * @param dims Dimension of the vectors to be generated.
     * @return The generated vector stream.
     */
    public static DataStream<DenseVector[]> generateRandomContinuousVectorArrayStream(
            StreamExecutionEnvironment env, long numData, int arraySize, long seed, int dims) {
        return env.fromParallelCollection(
                        new NumberSequenceIterator(1L, numData), BasicTypeInfo.LONG_TYPE_INFO)
                .map(new GenerateRandomContinuousVectorArrayFunction(seed, dims, arraySize));
    }

    private static class GenerateRandomContinuousVectorArrayFunction
            extends RichMapFunction<Long, DenseVector[]> {
        private final int dims;
        private final long initSeed;
        private final int arraySize;
        private Random random;

        private GenerateRandomContinuousVectorArrayFunction(
                long initSeed, int dims, int arraySize) {
            this.dims = dims;
            this.initSeed = initSeed;
            this.arraySize = arraySize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            int index = getRuntimeContext().getIndexOfThisSubtask();
            random = new Random(Tuple2.of(initSeed, index).hashCode());
        }

        @Override
        public DenseVector[] map(Long value) {
            DenseVector[] result = new DenseVector[arraySize];
            for (int i = 0; i < arraySize; i++) {
                result[i] = new DenseVector(dims);
                for (int j = 0; j < dims; j++) {
                    result[i].values[j] = random.nextDouble();
                }
            }
            return result;
        }
    }
}
