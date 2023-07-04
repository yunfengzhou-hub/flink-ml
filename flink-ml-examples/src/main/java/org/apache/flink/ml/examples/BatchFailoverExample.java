package org.apache.flink.ml.examples;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BatchFailoverExample {
    public static boolean isFailed;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        isFailed = false;

        DataStream<Long> stream = env.fromSequence(0, 10);
        stream = stream.map(new MyMapFunction<>("id"));
        stream.print();
        env.execute();
    }

    public static class MyMapFunction<T> implements MapFunction<T, T> {
        private final String id;

        public MyMapFunction(String id) {
            this.id = id;
        }

        @Override
        public T map(T t) throws Exception {
            if (t.equals(5L) && !isFailed) {
                isFailed = true;
                System.out.println("fail");
                throw new RuntimeException();
            }
            Thread.sleep(100);
            System.out.println(id + " " + t);
            return t;
        }
    }
}
