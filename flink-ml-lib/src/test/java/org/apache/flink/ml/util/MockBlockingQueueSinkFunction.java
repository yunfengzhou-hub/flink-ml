package org.apache.flink.ml.util;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MockBlockingQueueSinkFunction<T> implements SinkFunction<T> {
    private final String id;

    public MockBlockingQueueSinkFunction(String id) {
        this.id = id;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        TestBlockingQueueManager.offer(id, value);
    }
}
