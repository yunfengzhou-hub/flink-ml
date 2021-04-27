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

package org.apache.flink.ml.common.function;

import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.common.utils.PipelineUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class ValidationTest {
    StreamExecutionEnvironment env;
    StreamTableEnvironment tEnv;

    @Before
    public void setup(){
        env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        tEnv = StreamTableEnvironment.create(env, settings);
    }

    // Function should be stateless, i.e., not rely on Tables stored in Environment
    @Test(expected = ValidationException.class)
    public void testTableEnv() throws Exception {
        DataStream<TestType.Order> orderA =
                env.fromCollection(
                        Arrays.asList(
                                new TestType.Order(1L, "beer", 3),
                                new TestType.Order(1L, "diaper", 4),
                                new TestType.Order(3L, "rubber", 2)));
        tEnv.createTemporaryView("OrderA", orderA, $("user"), $("product"), $("amount"));

        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return tableEnvironment.sqlQuery("SELECT * FROM OrderA");
            }
        });

        PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
    }

    @Test(expected = ValidationException.class)
    public void testGrammarUnion() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.union(table);
            }
        });

        PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
    }
}
