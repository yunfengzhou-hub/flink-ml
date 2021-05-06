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

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.common.utils.PipelineUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.junit.Assert.assertEquals;

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
                                new TestType.Order(1L, "beer", 3L),
                                new TestType.Order(1L, "diaper", 4L),
                                new TestType.Order(3L, "rubber", 2L)));
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

    @Test(expected = Exception.class)
    public void testGrammarUnion() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                Table table1 = table.select($("user"), $("product").repeat(2).as("product"), $("amount"));
                return table1.union(table);
            }
        });

        PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
    }

    @Test(expected = Exception.class)
    public void testGroupBy() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.groupBy($("user")).select($("user"));
            }
        });

        PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.User.class, false);
    }

    @Test(expected = TableException.class)
    public void testOrderBy() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.orderBy($("time").asc());
            }
        });
        PipelineUtils.toFunction(pipeline, TestType.Meeting.class, TestType.Meeting.class);
    }

    @Test(expected = Exception.class)
    public void testWindow() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.window(Tumble.over(lit(5).minutes()).on($("time")).as("time")).groupBy($("time")).select($("*"));
            }
        });
        PipelineUtils.toFunction(pipeline, TestType.Meeting.class, TestType.Meeting.class);
    }

    @Test(expected = Exception.class)
    public void testGrammarIn() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                Table table1 = table.select($("product").repeat(2).as("product"));
                return table.select($("*")).where($("product").in(table1));
            }
        });

        PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
    }

    @Test(expected = Exception.class)
    public void testGrammarMinus() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.minus(table);
            }
        });

        PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
    }

    @Test(expected = Exception.class)
    public void testGrammarInsert() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                table.executeInsert("OutOrders");
                return table;
            }
        });

        PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
    }

    @Test(expected = Exception.class)
    public void testJoin() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                Table table1 = table.select($("user").as("user1"), $("product").repeat(2).as("product2"));
                return table.join(table1).where($("user").isEqual($("user1"))).select($("user"), $("product"), $("amount"));
            }
        });

        PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
    }

    @Test(expected = Exception.class)
    public void testRow() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.select($("*"));
            }
        });

        PipelineUtils.toFunction(pipeline, Row.class, Row.class);
    }

    @Test(expected = Exception.class)
    public void testTuple() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.select($("*"));
            }
        });

        PipelineUtils.toFunction(pipeline, Tuple1.class, Tuple1.class);
    }
}
