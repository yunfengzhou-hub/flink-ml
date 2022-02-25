package org.apache.flink.ml.util;

import org.junit.rules.ExternalResource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"unchecked"})
public class TestBlockingQueueManager extends ExternalResource {
    private static final Map<String, BlockingQueue> queueMap = new HashMap<>();
    private static long counter = 0;

    public static synchronized String createBlockingQueue() {
        String id = String.valueOf(counter);
        queueMap.put(id, new LinkedBlockingQueue<>());
        counter ++;
        return id;
    }

    public static <T> void offerAll(String id, T... values) throws InterruptedException {
        for (T value: values) {
            offer(id, value);
        }
    }

    public static <T> void offer(String id, T value) throws InterruptedException {
        offer(id, value, 1, TimeUnit.MINUTES);
    }

    public static <T> void offer(String id, T value, long timeout, TimeUnit unit) throws InterruptedException {
       boolean success = queueMap.get(id).offer(value, timeout, unit);
       if (!success) {
           throw new RuntimeException("Failed to enqueue " + value + " to blocking queue " + id + ".");
       }
    }

    public static <T> List<T> poll(String id, int num) throws InterruptedException {
        List<T> result = new ArrayList<>();
        for (int i = 0; i < num; i ++) {
            result.add(poll(id));
        }
        return result;
    }

    public static <T> T poll(String id) throws InterruptedException {
        return poll(id, 1, TimeUnit.MINUTES);
    }

    public static <T> T poll(String id, long timeout, TimeUnit unit) throws InterruptedException {
        return (T) queueMap.get(id).poll(timeout, unit);
    }

    public static void deleteBlockingQueue(String id) {
        queueMap.remove(id);
    }
}
