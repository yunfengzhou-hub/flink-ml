package org.apache.flink.ml.common.function;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

public class DataStreamValidationTest {
    @Test(expected = IllegalArgumentException.class)
    public void testStatefulFunction() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.fromElements("hello")
                .map(new RichMapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        return s.toUpperCase();
                    }
                });
        new EmbedStreamFunction<>(stream);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultipleSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> input1 = env.fromElements(1);
        DataStream<Integer> input2 = env.fromElements(1);
        DataStream<Integer> unioned = input1.union(input2);
        new EmbedStreamFunction<>(unioned);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWindowFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> stream = env.fromElements(1)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1);

        new EmbedStreamFunction<>(stream);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeyedFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> stream = env.fromElements(1)
                .keyBy((KeySelector<Integer, Object>) integer -> integer)
                .sum(1);

        new EmbedStreamFunction<>(stream);
    }

    @Test(expected = Exception.class)
    public void testIterate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> source = env.fromElements(1);
        IterativeStream<Integer> stream = source
                .map(x -> x + 1)
                .iterate();
        DataStream<Integer> feedback = stream.filter((FilterFunction<Integer>) integer -> integer>0);
        stream.closeWith(feedback);
        DataStream<Integer> result = stream.filter((FilterFunction<Integer>) integer -> integer<0);

        new EmbedStreamFunction<>(result);
    }
}
