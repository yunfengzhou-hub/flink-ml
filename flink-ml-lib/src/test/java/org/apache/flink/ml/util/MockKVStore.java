package org.apache.flink.ml.util;

import java.util.HashMap;
import java.util.Map;

/** Class that manages global key-value pairs used in unit tests. */
@SuppressWarnings({"unchecked"})
public class MockKVStore {
    private static final Map<String, Object> map = new HashMap<>();

    private static long counter = 0;

    /**
     * Returns a prefix string to make sure key-value pairs created in different test cases would
     * not have the same key.
     */
    public static synchronized String createNonDuplicatePrefix() {
        String prefix = Long.toString(counter);
        counter++;
        return prefix;
    }

    public static <T> T get(String key) {
        return (T) map.get(key);
    }

    public static <T> void set(String key, T value) {
        map.put(key, value);
    }

    public static boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public static void remove(String key) {
        map.remove(key);
    }
}
