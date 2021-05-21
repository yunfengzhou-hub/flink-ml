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
import org.apache.flink.ml.common.function.types.ExpandedOrder;
import org.apache.flink.ml.common.function.types.Order;
import org.apache.flink.ml.common.utils.PipelineUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
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

        Order data = new Order();
        pipelineEndToEndAssertEquals(pipeline, env, tEnv, Order.class, Order.class, data, data);
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

        Order inputData = new Order(1L, "product", 1L);
        Order outputData = new Order(2L, "productproduct", 3L);
        pipelineEndToEndAssertEquals(pipeline, env, tEnv, Order.class, Order.class, inputData, outputData);

    }

    @Test
    public void testRepeatExecution() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NopTransformer());

        StreamFunction<Order, Order> function = PipelineUtils.toFunction(pipeline, env, tEnv, Order.class, Order.class);
        Order data = new Order();

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

        StreamFunction<Order, Order> function =
                PipelineUtils.toFunction(filename, env, tEnv, Order.class, Order.class);

        Order data = new Order();
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

        Order data = new Order();
        pipelineEndToEndAssertEquals(pipeline, env, tEnv, Order.class, Order.class, data);
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

        ExpandedOrder input = new ExpandedOrder();
        Order output = new Order();
        pipelineEndToEndAssertEquals(pipeline, env, tEnv, ExpandedOrder.class, Order.class, input, output);
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

        Order data = new Order();
        pipelineEndToEndAssertEquals(pipeline, env, tEnv, Order.class, Order.class, data, data, data);
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

        Order data = new Order();
        pipelineEndToEndAssertEquals(pipeline, env, tEnv, Order.class, Order.class, data, data);
    }

    @Test
    public void testSQL() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                tableEnvironment.dropTemporaryView("tablename");
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

        Order data = new Order();
        pipelineEndToEndAssertEquals(pipeline, env, tEnv, Order.class, Order.class, data);
    }

    @Test
    public void testCallUDF() throws Exception {
        tEnv.createTemporarySystemFunction("myMap", MyMapFunction.class);

        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                tEnv.dropTemporaryView("MyTable");
                tEnv.createTemporaryView("MyTable", table);
                return tEnv.sqlQuery("SELECT myMap(product, 1, 4) AS product, user, amount FROM MyTable");
            }
        });

        Order inputData = new Order(1L, "product", 1L);
        Order outputData = new Order(1L, "rod", 1L);

        pipelineEndToEndAssertEquals(pipeline, env, tEnv, Order.class, Order.class, inputData, outputData);
    }

    @Test
    public void testIndependentFromEnv() throws Exception {
        tEnv.createTemporarySystemFunction("myMap", MyMapFunction.class);

        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                tEnv.dropTemporaryView("MyTable");
                tEnv.createTemporaryView("MyTable", table);
                return tEnv.sqlQuery("SELECT myMap(product, 1, 4) AS product, user, amount FROM MyTable");
            }
        });

        Order inputData = new Order(1L, "product", 1L);
        Order outputData = new Order(1L, "rod", 1L);

        pipelineEndToEndAssertEquals(pipeline, env, tEnv, Order.class, Order.class, inputData, outputData);
        StreamFunction<Order, Order> function =
                PipelineUtils.toFunction(pipeline, env, tEnv, Order.class, Order.class);

        // generated function should be independent from external environment.
        tEnv.dropTemporarySystemFunction("myMap");

        assertEquals(Collections.singletonList(outputData), function.apply(inputData));
    }

    public static class MyMapFunction extends ScalarFunction {
        public String eval(String s, Integer begin, Integer end) {
            return s.substring(begin, end);
        }
    }

    public <IN, OUT> void pipelineEndToEndAssertEquals(
            Pipeline pipeline,
            StreamExecutionEnvironment env,
            StreamTableEnvironment tEnv,
            Class<IN> inClass,
            Class<OUT> outClass,
            IN input, OUT... expectedOutput) throws Exception {
        StreamFunction<IN, OUT> function =
                (StreamFunction<IN, OUT>) PipelineUtils.toFunction(pipeline, env, tEnv, inClass, outClass);

        function = StreamFunction.deserialize(function.serialize());
        assertEquals(Arrays.asList(expectedOutput), function.apply(input));
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
