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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

public class PipelineExecutionTest {
    StreamExecutionEnvironment env;
    StreamTableEnvironment tEnv;
    String filename = "test-pipeline.json";

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        tEnv = StreamTableEnvironment.create(env, settings);
    }

    @Test
    public void testBasicCorrectness() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NopTransformer());

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order();
        assertEquals(Collections.singletonList(data), function.apply(data));

    }

    @Test
    public void testArithmetic() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.select(
                        $("user").plus(1).as("user"),
                        $("product").repeat(2).as("product"),
                        $("amount").times(3).as("amount"));
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order inputData = new TestType.Order(
                1L,
                "product",
                1L
        );
        TestType.Order outputData = new TestType.Order(
                2L,
                "productproduct",
                3L
        );
        assertEquals(Collections.singletonList(outputData), function.apply(inputData));

    }

    @Test
    public void testRepeatExecution() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NopTransformer());

        StreamFunction<TestType.Order, TestType.Order> function = PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
        TestType.Order data = new TestType.Order();

        for(int i = 0; i < 10; i++){
            function.apply(data);
        }
        assertEquals(Collections.singletonList(data), function.apply(data));

    }

    @Test
    public void testJsonFile() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NopTransformer());

        String pipelineJson = pipeline.toJson();
        PrintWriter out = new PrintWriter(filename);
        out.println(pipelineJson);
        out.close();

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(filename, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order();
        assertEquals(Collections.singletonList(data), function.apply(data));
    }

    @Test
    public void testGrammarWhere() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.select($("*")).where($("product").isEqual("napkin"));
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order();
        assertEquals(Collections.emptyList(), function.apply(data));
    }

    @Test
    public void testGrammarColumn() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.dropColumns($("price"));
            }
        });

        StreamFunction<TestType.ExpandedOrder, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.ExpandedOrder.class, TestType.Order.class);

        TestType.ExpandedOrder input = new TestType.ExpandedOrder();
        TestType.Order output = new TestType.Order();
        assertEquals(Collections.singletonList(output), function.apply(input));
    }

    @Test
    public void testGrammarUnionAll() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.unionAll(table);
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order();
        assertEquals(Arrays.asList(data, data), function.apply(data));
    }

    @Test
    public void testInternalTable() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                tableEnvironment.createTemporaryView("table", table);
                return table;
            }
        });
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return tableEnvironment.from("table");
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order();
        assertEquals(Collections.singletonList(data), function.apply(data));
    }

    @Test
    public void testSQL() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                tableEnvironment.createTemporaryView("tablename", table);
                return table;
            }
        });
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return tableEnvironment.sqlQuery("SELECT * FROM tablename WHERE amount > 1");
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order();
        assertEquals(Collections.emptyList(), function.apply(data));
    }

    @After
    public void teardown(){
        File f = new File(filename);
        if(f.exists()){
            if(!f.delete()){
                throw new UnsupportedOperationException("Cannot delete test file: " + filename);
            }
        }
    }
}
