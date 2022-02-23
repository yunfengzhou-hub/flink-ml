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

package org.apache.flink.ml.util;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.HashMap;
import java.util.Map;

/** A subclass of {@link MetricReporter} that outputs metrics to a global map. */
public class TestMetricReporter implements MetricReporter, Scheduled {
    private static final Map<String, Object> map = new HashMap<>();
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
            set(getGlobalKey(prefix, entry.getKey()), entry.getValue().getValue());
        }
    }

    public static void set(String key, Object value) {
        map.put(key, value);
    }

    public static Object get(String key) {
        return map.get(key);
    }

    public static boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public static void remove(String key) {
        map.remove(key);
    }

    public static String getGlobalKey(String prefix, String metricName) {
        return prefix + "-" + metricName;
    }
}
