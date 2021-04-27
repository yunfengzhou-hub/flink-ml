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
import java.util.List;
import java.util.function.Function;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

public class CorrectnessTest {
    StreamExecutionEnvironment env;
    StreamTableEnvironment tEnv;
    String filename = "test-pipeline.json";

    @Before
    public void setup(){

        env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        tEnv = StreamTableEnvironment.create(env, settings);
    }

    @Test
    public void testBasicCorrectness() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NopTransformer());

        Function<TestType.Order, List<TestType.Order>> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order(1L, "beer", 3);
        assertEquals(Collections.singletonList(data), function.apply(data));

    }

    @Test
    public void testRepeatCompilation() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NopTransformer());

        Function<TestType.Order, List<TestType.Order>> function = PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
        for(int i = 0; i < 100; i++){
            function = PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
        }

        TestType.Order data = new TestType.Order(1L, "beer", 3);
        assertEquals(Collections.singletonList(data), function.apply(data));

    }

    @Test
    public void testRepeatExecution() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NopTransformer());

        Function<TestType.Order, List<TestType.Order>> function = PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);
        TestType.Order data = new TestType.Order(1L, "beer", 3);

        for(int i = 0; i < 100; i++){
            function.apply(data);
        }
        assertEquals(Collections.singletonList(data), function.apply(data));

    }

    @Test
    public void testEnd2End() throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new NopTransformer());

        String pipelineJson = pipeline.toJson();
        PrintWriter out = new PrintWriter(filename);
        out.println(pipelineJson);
        out.close();

        Function<TestType.Order, List<TestType.Order>> function =
                PipelineUtils.toFunction(filename, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order(1L, "beer", 3);
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

        Function<TestType.Order, List<TestType.Order>> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order(1L, "beer", 3);
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

        Function<TestType.ExpandedOrder, List<TestType.Order>> function =
                PipelineUtils.toFunction(pipeline, TestType.ExpandedOrder.class, TestType.Order.class);

        TestType.ExpandedOrder input = new TestType.ExpandedOrder(1L, "beer", 3, 1.0);
        TestType.Order output = new TestType.Order(1L, "beer", 3);
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

        Function<TestType.Order, List<TestType.Order>> function =
                PipelineUtils.toFunction(pipeline, TestType.Order.class, TestType.Order.class);

        TestType.Order data = new TestType.Order(1L, "beer", 3);
        assertEquals(Arrays.asList(data, data), function.apply(data));
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
