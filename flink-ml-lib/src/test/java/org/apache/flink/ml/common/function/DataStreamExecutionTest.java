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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.common.function.types.Order;
import org.apache.flink.ml.common.utils.PipelineUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"unused"})
public class DataStreamExecutionTest {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    @Before
    public void createEnvironment() {
        env = StreamExecutionEnvironment.createLocalEnvironment();
    }

    @Test
    public void testBasicCorrectness() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase);
        dataStreamEndToEndAssertEquals(stream, "hello", "HELLO");
    }

    @Test
    public void testChainedOperation() throws Exception {
        DataStream<String> stream = env.fromElements(1)
                .map(x -> x + "x")
                .map(String::toUpperCase);
        dataStreamEndToEndAssertEquals(stream, 1, "1X");
    }

    @Test
    public void testConnectedStream() throws Exception {
        DataStream<Integer> input = env.fromElements(1);
        DataStream<Integer> branch1 = input.map(x -> x+1);
        DataStream<Integer> branch2 = input.map(x -> x-1);
        DataStream<Integer> connected = branch1.connect(branch2).map(new CoMapFunction<Integer, Integer, Integer>() {
            @Override
            public Integer map1(Integer value) throws Exception {
                if(value<1) value+=2;
                return value;
            }

            @Override
            public Integer map2(Integer value) throws Exception {
                if(value<1) value+=2;
                return value;
            }
        });
        dataStreamEndToEndAssertEquals(connected, 1, 2, 2);
    }

    @Test
    public void testUnionStream() throws Exception {
        DataStream<Integer> input = env.fromElements(1);
        DataStream<Integer> branch1 = input.map(x -> x+1);
        DataStream<Integer> branch2 = input.map(x -> x-1);
        DataStream<Integer> unioned = branch1.union(branch2).map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                if(value<1) value+=2;
                return value;
            }
        });
        dataStreamEndToEndAssertEquals(unioned, 1, 2, 2);
    }

    @Test
    public void testFlatMap() throws Exception {
        DataStream<Integer> stream = env.fromElements(1)
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(
                            Integer integer,
                            Collector<Integer> collector) throws Exception {
                        collector.collect(integer+1);
                        collector.collect(integer+1);
                    }
                });

        dataStreamEndToEndAssertEquals(stream, 1, 2, 2);
    }

    @Test
    public void testFilter() throws Exception {
        DataStream<Integer> stream = env.fromElements(1)
                .filter((FilterFunction<Integer>) integer -> false);

        dataStreamEndToEndAssertEquals(stream, 1);
    }

    @Test
    public void testCoFlatMap() throws Exception {
        DataStream<Integer> input = env.fromElements(1);
        DataStream<Integer> branch1 = input.map(x -> x+1);
        DataStream<Integer> branch2 = input.map(x -> x-1);
        DataStream<Integer> connected = branch1.connect(branch2).flatMap(new CoFlatMapFunction<Integer, Integer, Integer>() {
            @Override
            public void flatMap1(Integer value, Collector<Integer> out) throws Exception {
                if(value<1) value+=2;
                out.collect(value);
            }

            @Override
            public void flatMap2(Integer value, Collector<Integer> out) throws Exception {
                if(value<1) value+=2;
                out.collect(value);
            }
        });
        dataStreamEndToEndAssertEquals(connected, 1, 2, 2);
    }

    @Test
    public void testProject() throws Exception {
        DataStream<Tuple3<String, Integer, Boolean>> input = env.fromElements(new Tuple3<>("hello", 1, true));
        DataStream<Tuple2<Boolean, String>> stream = input.project(2, 0);

        dataStreamEndToEndAssertEquals(stream, new Tuple3<>("hello", 1, true), new Tuple2<>(true, "hello"));
    }

    @Test
    public void testStartNewChain() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase)
                .map(String::toUpperCase)
                .startNewChain()
                .map(String::toUpperCase)
                .map(String::toUpperCase);
        dataStreamEndToEndAssertEquals(stream, "hello", "HELLO");;
    }

    @Test
    public void testDisableChaining() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase)
                .map(String::toUpperCase)
                .disableChaining()
                .map(String::toUpperCase)
                .map(String::toUpperCase);
        dataStreamEndToEndAssertEquals(stream, "hello", "HELLO");;
    }

    @Test
    public void testNotClearOperators() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase);
        dataStreamEndToEndAssertEquals(stream, "hello", "HELLO");
        dataStreamEndToEndAssertEquals(stream, "hello", "HELLO");
        dataStreamEndToEndAssertEquals(stream, "hello", "HELLO");
    }

    @Test
    public void testMultipleInvocation() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase);
        StreamFunction<String, String> function = new StreamFunction<>(stream);
        assertEquals(Collections.singletonList("HELLO"), function.apply("hello"));
        assertEquals(Collections.singletonList("HELLO"), function.apply("hello"));
        assertEquals(Collections.singletonList("HELLO"), function.apply("hello"));
    }

    @Test
    public void testOperatorID() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase).uid("operator 6")
                .map(String::toLowerCase).uid("operator 5")
                .map(String::toUpperCase).uid("operator 4")
                .map(String::toLowerCase).uid("operator 3")
                .map(String::toUpperCase).uid("operator 2")
                .map(String::toLowerCase).uid("operator 1")
                .map(String::toUpperCase);
        dataStreamEndToEndAssertEquals(stream, "hello", "HELLO");;
    }

    @Test
    public void testIrrelevantBranch() throws Exception {
        DataStream<String> input = env.fromElements("hello");
        DataStream<String> stream = input.map(String::toUpperCase);
        DataStream<String> stream2 = stream.map(x -> x+" " +x);

        input.map((MapFunction<String, Object>) s -> {
            throw new Exception();
        });

        stream.map((MapFunction<String, Object>) s -> {
            throw new Exception();
        });

        stream2.map((MapFunction<String, Object>) s -> {
            throw new Exception();
        });

        dataStreamEndToEndAssertEquals(stream, "hello", "HELLO");
        dataStreamEndToEndAssertEquals(stream2, "hello", "HELLO HELLO");
    }

    @Test
    public void testMixTable() throws Exception {
        DataStream<Order> stream = env.fromElements(new Order())
                .map((MapFunction<Order, Order>) order -> {
                    order.amount ++;
                    return order;
                });
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(stream)
                .select($("user").plus(1L).as("user"), $("amount"), $("product"));
        DataStream<Order> outStream = tEnv.toAppendStream(table, Order.class);

        Order inputData = new Order(
                1L,
                "product",
                1L
        );
        Order outputData = new Order(
                2L,
                "product",
                2L
        );

        dataStreamEndToEndAssertEquals(outStream, inputData, outputData);
    }

    @Test(expected = Exception.class)
    public void testRuntimeError() throws Exception {
        DataStream<String> stream = env.fromElements("hello")
                .map((MapFunction<String, String>) s -> s.substring(10));
        new StreamFunction<>(stream).apply("hello");
    }

    @SafeVarargs
    public final <IN, OUT> void dataStreamEndToEndAssertEquals(DataStream<OUT> stream, IN input, OUT... expectedOutput) throws Exception {
        StreamFunction<IN, OUT> function = new StreamFunction<>(stream);
        function = PipelineUtils.deserializeFunction(PipelineUtils.serializeFunction(function));
        assertEquals(Arrays.asList(expectedOutput), function.apply(input));
    }

    @After
    public void clearGraph() {
        env.getStreamGraph();
    }
}
