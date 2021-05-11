package org.apache.flink.ml.common.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NoStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> stream = env.fromElements(1,2,3);
        final int[] sum = {0};
        stream.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer integer) throws Exception {
                sum[0] += integer;
                return sum[0];
            }
        }).print();
        env.execute();
    }
}
