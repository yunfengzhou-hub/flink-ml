package org.apache.flink.ml.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

public class MockBlockingQueueSourceFunction<T> implements SourceFunction<T> {
    private static final Map<String, BlockingQueue<String>> queueMap = new HashMap<>();
    private final String id;
    private final Function<String, T> deserializer;
    private boolean isRunning = true;

    public MockBlockingQueueSourceFunction(String id, Function<String, T> deserializer) {
        this.id = id;
        this.deserializer = deserializer;
    }

    public static BlockingQueue<String> getQueue(String id) {
        if (!queueMap.containsKey(id)) {
            queueMap.put(id, new LinkedBlockingQueue<>());
        }
        return queueMap.get(id);
    }

    @Override
    public void run(SourceContext<T> context) throws Exception {
        BlockingQueue<String> queue = getQueue(id);
        while (isRunning) {
            T item = deserializer.apply(queue.take());
            context.collect(item);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
