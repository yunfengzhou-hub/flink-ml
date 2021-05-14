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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.core.Pipeline;
import org.apache.flink.ml.common.utils.PipelineUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
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
                Table table1 = table.select($("user"), $("product"), $("amount").times(2).as("amount"));
                return table1.unionAll(table);
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order inputData = new TestType.Order(1L, "product", 1L);
        TestType.Order outputData = new TestType.Order(1L, "product", 2L);

        assertEquals(Arrays.asList(outputData, inputData), function.apply(inputData));
    }

    @Test
    public void testGrammarUnion() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.union(table);
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order(1L, "product", 1L);

        assertEquals(Collections.singletonList(data), function.apply(data));
    }

    @Test
    public void testGrammarIn() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                Table table1 = table.select($("amount"), $("product"), $("user").plus(1).as("user"));
                Table table2 = table.select($("amount"), $("product"), $("user").plus(2).as("user"));
                Table table3 = table.union(table1).union(table2);
                return table3.select($("*")).where($("user").in(table2.select($("user"))));
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order inputData = new TestType.Order(1L, "product", 1L);
        TestType.Order outputData = new TestType.Order(3L, "product", 1L);

        assertEquals(Collections.singletonList(outputData), function.apply(inputData));
    }

    @Test
    public void testGrammarMinus() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                Table table1 = table.select($("amount"), $("product"), $("user").plus(1).as("user"));
                Table table2 = table.select($("amount"), $("product"), $("user").plus(2).as("user"));
                Table table3 = table.union(table1).union(table2);
                return table3.minus(table);
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data1 = new TestType.Order(1L, "product", 1L);
        TestType.Order data2 = new TestType.Order(2L, "product", 1L);
        TestType.Order data3 = new TestType.Order(3L, "product", 1L);

        assertEquals(Arrays.asList(data3,data2), function.apply(data1));
    }

    @Test
    public void testGrammarJoin() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                Table table1 = table.select(
                        $("user").as("user1"),
                        $("product").as("product1"),
                        $("amount").as("amount1")
                );
                return table.join(table1)
                        .where($("user").isEqual($("user1")))
                        .select($("user"), $("product"), $("amount"));
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order(1L, "product", 1L);

        assertEquals(Collections.singletonList(data), function.apply(data));
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

    @Test
    public void testGroupBy() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.unionAll(table)
                        .groupBy(
                                $("product")
                        ).select(
                                $("user")
                                        .sum()
                                        .as("user"),
                                $("product"),
                                $("amount")
                                        .sum()
                                        .as("amount")
                        );
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order inputData = new TestType.Order(1L, "product", 1L);
        TestType.Order outputData = new TestType.Order(2L, "product", 2L);
        assertEquals(Collections.singletonList(outputData), function.apply(inputData));
    }

    @Test
    public void testDistinct() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.unionAll(table)
                        .distinct();
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order();
        assertEquals(Collections.singletonList(data), function.apply(data));
    }

    @Test
    public void testOrderBy() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                Table table1 = table.select($("amount"), $("product"), $("user").plus(1).as("user"));
                Table table2 = table.select($("amount"), $("product"), $("user").plus(2).as("user"));
                return table.union(table1).union(table2);
            }
        });
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.orderBy($("user").asc());
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data1 = new TestType.Order(1L, "product", 1L);
        TestType.Order data2 = new TestType.Order(2L, "product", 1L);
        TestType.Order data3 = new TestType.Order(3L, "product", 1L);

        assertEquals(Arrays.asList(data1, data2, data3), function.apply(data1));
    }

    @Test
    public void testOrderByFetch() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                Table table1 = table.select($("amount"), $("product"), $("user").plus(1).as("user"));
                Table table2 = table.select($("amount"), $("product"), $("user").plus(2).as("user"));
                return table.union(table1).union(table2);
            }
        });
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                return table.orderBy($("user").desc()).fetch(2);
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data1 = new TestType.Order(1L, "product", 1L);
        TestType.Order data2 = new TestType.Order(2L, "product", 1L);
        TestType.Order data3 = new TestType.Order(3L, "product", 1L);

        assertEquals(Arrays.asList(data3, data2), function.apply(data1));
    }

    @Test
    public void testRowMap() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NoParamsTransformer() {
            @Override
            public Table transform(TableEnvironment tableEnvironment, Table table) {
                tableEnvironment.createTemporaryFunction("func", new MyMapFunction());
                System.out.println(new MyMapFunction().getKind());
                System.out.println(call("func", $("product")));
                return table.map(call("func", $("product")).as("product"));
            }
        });

        StreamFunction<TestType.Order, TestType.Order> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data1 = new TestType.Order(1L, "product", 1L);
        TestType.Order data2 = new TestType.Order(2L, "product", 1L);
        TestType.Order data3 = new TestType.Order(3L, "product", 1L);

        assertEquals(Arrays.asList(data3, data2), function.apply(data1));
    }

    public class MyMapFunction extends ScalarFunction {
        public Row eval(String a) {
            return Row.of(a, "pre-" + a);
        }

        @Override
        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.ROW(Types.STRING(), Types.STRING());
        }
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
