package org.apache.flink.ml.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MockBlockingQueueSourceFunction<T> implements SourceFunction<T> {
    private final String id;
    private boolean isRunning = true;

    public MockBlockingQueueSourceFunction(String id) {
        this.id = id;
    }

    @Override
    public void run(SourceContext<T> context) throws Exception {
        while (isRunning) {
            context.collect(TestBlockingQueueManager.poll(id));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
