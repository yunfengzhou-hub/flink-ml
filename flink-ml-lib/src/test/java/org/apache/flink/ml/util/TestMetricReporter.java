package org.apache.flink.ml.util;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.HashMap;
import java.util.Map;

/**
 * A subclass of {@link MetricReporter} that outputs metrics to in-memory kv store managed by {@link
 * MockKVStore}.
 */
public class TestMetricReporter implements MetricReporter, Scheduled {
    private String prefix;

    private final Map<String, Gauge<?>> gauges = new HashMap<>();

    @Override
    public void open(MetricConfig metricConfig) {
        prefix = metricConfig.getString("prefix", "");
        if (prefix.equals("")) {
            throw new IllegalArgumentException("Prefix not configured.");
        }
    }

    @Override
    public void close() {}

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup metricGroup) {
        synchronized (this) {
            if (metric instanceof Gauge) {
                gauges.put(metricName, (Gauge<?>) metric);
            } else {
                throw new IllegalArgumentException("Unsupported metric class " + metric.getClass());
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup metricGroup) {
        synchronized (this) {
            if (metric instanceof Gauge) {
                gauges.remove(metricName);
            } else {
                throw new IllegalArgumentException("Unsupported metric class " + metric.getClass());
            }
        }
    }

    @Override
    public void report() {
        for (Map.Entry<String, Gauge<?>> entry : gauges.entrySet()) {
            MockKVStore.set(getKey(prefix, entry.getKey()), entry.getValue().getValue());
        }
    }

    public static String getKey(String prefix, String metricName) {
        return prefix + "-" + metricName;
    }
}
