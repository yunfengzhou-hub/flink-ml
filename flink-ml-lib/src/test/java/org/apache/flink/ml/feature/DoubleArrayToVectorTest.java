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

package org.apache.flink.ml.feature;

import org.apache.flink.ml.feature.doublearray.DoubleArrayToVector;
import org.apache.flink.ml.feature.doublearray.VectorToDoubleArray;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** DoubleArrayToVectorTest. */
public class DoubleArrayToVectorTest extends AbstractTestBase {
    private static final Vector[] VECTOR_DATA = new Vector[] {Vectors.dense(0, 1, 2)};

    private static final Double[][] DOUBLE_ARRAY_DATA = new Double[][] {new Double[] {0., 1., 2.}};

    @Test
    public void testDoubleArrayToVector() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table input = tEnv.fromDataStream(env.fromElements(DOUBLE_ARRAY_DATA)).as("f0");

        DoubleArrayToVector transformer = new DoubleArrayToVector().setInputCols("f0");

        Table output = transformer.transform(input)[0];

        Row row = output.execute().collect().next();
        Vector vector = row.getFieldAs("f0");
        assertEquals(VECTOR_DATA[0], vector);
    }

    @Test
    public void testVectorToDoubleArray() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table input = tEnv.fromDataStream(env.fromElements(VECTOR_DATA)).as("f0");

        VectorToDoubleArray transformer = new VectorToDoubleArray().setInputCols("f0");

        Table output = transformer.transform(input)[0];

        Row row = output.execute().collect().next();
        Double[] doubles = row.getFieldAs("f0");
        assertArrayEquals(DOUBLE_ARRAY_DATA[0], doubles);
    }
}
