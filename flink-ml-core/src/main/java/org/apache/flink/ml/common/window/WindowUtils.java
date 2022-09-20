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
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
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
     * Applies windowAll() operation on the input stream.
     *
     * @param input The input data stream.
     * @param window The window that defines how input data would be sliced into batches.
     */
    public static <T, W extends org.apache.flink.streaming.api.windowing.windows.Window>
            AllWindowedStream<T, W> windowAll(DataStream<T> input, Window window) {
        AllWindowedStream<T, W> output;
        if (window instanceof BoundedWindow) {
            output = input.windowAll((WindowAssigner) EndOfStreamWindows.get());
        } else if (window instanceof TumbleWindow && ((TumbleWindow) window).countWindowSize > 0) {
            long countWindowSize = ((TumbleWindow) window).countWindowSize;
            output = (AllWindowedStream<T, W>) input.countWindowAll(countWindowSize);
        } else {
            output =
                    input.windowAll(
                            (WindowAssigner) WindowUtils.getDataStreamTimeWindowAssigner(window));
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
                map.put("timeWindowSize", tumbleWindow.timeWindowSize.toString());
            }
            if (tumbleWindow.timeWindowOffset != null) {
                map.put("timeWindowOffset", tumbleWindow.timeWindowOffset.toString());
            }
            map.put("isEventTime", tumbleWindow.isEventTime);
            map.put("countWindowSize", tumbleWindow.countWindowSize);
            return map;
        } else if (value instanceof SessionWindow) {
            SessionWindow sessionWindow = (SessionWindow) value;
            if (sessionWindow.gap != null) {
                map.put("gap", sessionWindow.gap.toString());
            }
            map.put("isEventTime", sessionWindow.isEventTime);
            return map;
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported %s subclass: %s", Window.class, value.getClass()));
        }
    }

    public static Window jsonDecode(Object json) {
        Map<String, Object> map = (Map<String, Object>) json;
        String classString = (String) map.get("class");
        if (classString.equals(BoundedWindow.class.getCanonicalName())) {
            return BoundedWindow.get();
        } else if (classString.equals(TumbleWindow.class.getCanonicalName())) {
            Duration timeWindowSize = null;
            if (map.containsKey("timeWindowSize")) {
                timeWindowSize = Duration.parse((String) map.get("timeWindowSize"));
            }
            Duration timeWindowOffset = null;
            if (map.containsKey("timeWindowOffset")) {
                timeWindowOffset = Duration.parse((String) map.get("timeWindowOffset"));
            }
            long countWindowSize = ((Number) map.get("countWindowSize")).longValue();
            boolean isEventTime = (boolean) map.get("isEventTime");

            TumbleWindow tumbleWindow = TumbleWindow.over(countWindowSize);
            tumbleWindow.timeWindowOffset = timeWindowOffset;
            tumbleWindow.timeWindowSize = timeWindowSize;
            tumbleWindow.isEventTime = isEventTime;

            return tumbleWindow;
        } else if (classString.equals(SessionWindow.class.getCanonicalName())) {
            Duration gap = null;
            if (map.containsKey("gap")) {
                gap = Duration.parse((String) map.get("gap"));
            }
            boolean isEventTime = (boolean) map.get("isEventTime");

            SessionWindow sessionWindow = SessionWindow.withGap(gap);
            sessionWindow.isEventTime = isEventTime;

            return sessionWindow;
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported %s subclass: %s", Window.class, classString));
        }
    }
}
