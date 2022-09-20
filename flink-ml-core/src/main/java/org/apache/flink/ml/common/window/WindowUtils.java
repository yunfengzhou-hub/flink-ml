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

package org.apache.flink.ml.common.window;

import org.apache.flink.ml.common.datastream.EndOfStreamWindows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Utility class for operations related to the window. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class WindowUtils {
    /**
     * Applies allWindow() and process() operation on the input stream.
     *
     * @param input The input data stream.
     * @param window The window that defines how input data would be sliced into batches.
     * @param function The user defined process function.
     */
    public static <IN, OUT> SingleOutputStreamOperator<OUT> allWindowProcess(
            DataStream<IN> input,
            Window window,
            ProcessAllWindowFunction<IN, OUT, ? extends Window> function) {
        SingleOutputStreamOperator<OUT> output;
        if (window instanceof BoundedWindow) {
            output = input.windowAll(EndOfStreamWindows.get()).process(function);
        } else if (window instanceof TumbleWindow && ((TumbleWindow) window).countWindowSize > 0) {
            long countWindowSize = ((TumbleWindow) window).countWindowSize;
            output =
                    input.countWindowAll(countWindowSize)
                            .process((ProcessAllWindowFunction) function);
        } else {
            output =
                    input.windowAll(WindowUtils.getDataStreamTimeWindowAssigner(window))
                            .process((ProcessAllWindowFunction) function);
        }
        return output;
    }

    private static WindowAssigner<Object, TimeWindow> getDataStreamTimeWindowAssigner(
            Window window) {
        if (window instanceof TumbleWindow) {
            TumbleWindow tumbleWindow = (TumbleWindow) window;
            long size = tumbleWindow.timeWindowSize.toMillis();
            long offset = tumbleWindow.timeWindowOffset.toMillis();
            if (((TumbleWindow) window).isEventTime) {
                return TumblingEventTimeWindows.of(
                        Time.milliseconds(size), Time.milliseconds(offset));
            } else {
                return TumblingProcessingTimeWindows.of(
                        Time.milliseconds(size), Time.milliseconds(offset));
            }
        } else if (window instanceof SessionWindow) {
            long gap = ((SessionWindow) window).gap.toMillis();
            if (((SessionWindow) window).isEventTime) {
                return EventTimeSessionWindows.withGap(Time.milliseconds(gap));
            } else {
                return ProcessingTimeSessionWindows.withGap(Time.milliseconds(gap));
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported %s subclass: %s", Window.class, window.getClass()));
        }
    }

    public static Object jsonEncode(Window value) {
        Map<String, Object> map = new HashMap<>();
        map.put("class", value.getClass().getCanonicalName());
        if (value instanceof BoundedWindow) {
            return map;
        } else if (value instanceof TumbleWindow) {
            TumbleWindow tumbleWindow = (TumbleWindow) value;
            if (tumbleWindow.timeWindowSize != null) {
                map.put("timeWindowSize", tumbleWindow.timeWindowSize.toMillis());
            }
            map.put("timeWindowOffset", tumbleWindow.timeWindowOffset.toMillis());
            map.put("countWindowSize", tumbleWindow.countWindowSize);
            return map;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static Window jsonDecode(Object json) {
        Map<String, Object> map = (Map<String, Object>) json;
        String clazzString = (String) map.get("class");
        if (clazzString.equals(BoundedWindow.class.getCanonicalName())) {
            return BoundedWindow.get();
        } else if (clazzString.equals(TumbleWindow.class.getCanonicalName())) {
            long countWindowSize = (long) map.get("countWindowSize");
            Duration timeWindowSize = null;
            if (map.containsKey("timeWindowSize")) {
                long timeWindowSizeMillis = (long) map.get("timeWindowSize");
                timeWindowSize = Duration.ofMillis(timeWindowSizeMillis);
            }

            if (countWindowSize > 0) {
                return TumbleWindow.over(countWindowSize);
            } else {
                return TumbleWindow.over(timeWindowSize);
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
