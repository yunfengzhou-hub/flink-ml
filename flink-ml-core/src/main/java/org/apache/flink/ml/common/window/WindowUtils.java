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

import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import javax.activation.UnsupportedDataTypeException;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class WindowUtils {
    public static <IN, OUT> SingleOutputStreamOperator<OUT> allWindowProcess(
            DataStream<IN> dataStream,
            Window window,
            ProcessAllWindowFunction<IN, OUT, GlobalWindow> function,
            TypeInformation<OUT> outputTypeInfo,
            boolean isEventTime) {
        SingleOutputStreamOperator<OUT> output;
        if (window instanceof BoundedWindow) {
            output =
                    (SingleOutputStreamOperator<OUT>)
                            dataStream
                                    .windowAll(EndOfStreamWindows.get())
                                    .process(function, outputTypeInfo);
        } else if (window instanceof TumbleWindow && ((TumbleWindow) window).countWindowSize > 0) {
            long countWindowSize = ((TumbleWindow) window).countWindowSize;
            output = dataStream.countWindowAll(countWindowSize).process(function, outputTypeInfo);
        } else {
            output =
                    dataStream
                            .windowAll(WindowUtils.getDataStreamWindowAssigner(window, isEventTime))
                            .process(function, outputTypeInfo);
        }
        return output;
    }

    private static WindowAssigner getDataStreamWindowAssigner(Window window, boolean isEventTime) {
        if (window instanceof TumbleWindow) {
            TumbleWindow tumbleWindow = (TumbleWindow) window;
            long size = tumbleWindow.timeWindowSize.toMillis();
            long offset = tumbleWindow.timeWindowOffset.toMillis();
            if (isEventTime) {
                return TumblingEventTimeWindows.of(
                        Time.milliseconds(size), Time.milliseconds(offset));
            } else {
                return TumblingProcessingTimeWindows.of(
                        Time.milliseconds(size), Time.milliseconds(offset));
            }
        } else if (window instanceof SessionWindow) {
            long gap = ((SessionWindow) window).gap.toMillis();
            if (isEventTime) {
                return EventTimeSessionWindows.withGap(Time.milliseconds(gap));
            } else {
                return ProcessingTimeSessionWindows.withGap(Time.milliseconds(gap));
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported %s subclass: %s", Window.class, window.getClass()));
        }
    }

    public static boolean isRowTime(Table table, Window window) {
        ResolvedSchema schema = table.getResolvedSchema();
        boolean isEventTime = false;
        String timestampCol = null;
        if (window instanceof TumbleWindow) {
            timestampCol = ((TumbleWindow) window).timestampCol;
        } else if (window instanceof SessionWindow) {
            timestampCol = ((SessionWindow) window).timestampCol;
        }

        if (timestampCol != null) {
            Column.PhysicalColumn column =
                    (Column.PhysicalColumn)
                            schema.getColumns().get(schema.getColumnNames().indexOf(timestampCol));
            if (LogicalTypeChecks.isRowtimeAttribute(column.getDataType().getLogicalType())) {
                isEventTime = true;
            }
        } else {
            if (!schema.getWatermarkSpecs().isEmpty()) {
                isEventTime = true;
            }
        }
        return isEventTime;
    }

    public static Object jsonEncode(Window value) throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("class", value.getClass().getCanonicalName());
        if (value instanceof BoundedWindow) {
            return map;
        } else if (value instanceof TumbleWindow) {
            TumbleWindow tumbleWindow = (TumbleWindow) value;
            map.put("timestampCol", tumbleWindow.timestampCol);
            if (tumbleWindow.timeWindowSize != null) {
                map.put("timeWindowSize", tumbleWindow.timeWindowSize.toMillis());
            }
            map.put("timeWindowOffset", tumbleWindow.timeWindowOffset.toMillis());
            map.put("countWindowSize", tumbleWindow.countWindowSize);
            return map;
        } else {
            throw new UnsupportedDataTypeException();
        }
    }

    public static Window jsonDecode(Object json) throws IOException {
        Map<String, Object> map = (Map<String, Object>) json;
        String clazzString = (String) map.get("class");
        if (clazzString.equals(BoundedWindow.class.getCanonicalName())) {
            return BoundedWindow.get();
        } else if (clazzString.equals(TumbleWindow.class.getCanonicalName())) {
            String timestampCol = (String) map.get("timestampCol");
            long countWindowSize = (long) map.get("countWindowSize");
            Duration timeWindowSize = null;
            if (map.containsKey("timeWindowSize")) {
                long timeWindowSizeMillis = (long) map.get("timeWindowSize");
                timeWindowSize = Duration.ofMillis(timeWindowSizeMillis);
            }

            if (countWindowSize > 0) {
                return TumbleWindow.over(countWindowSize);
            } else {
                return TumbleWindow.over(timeWindowSize).on(timestampCol);
            }
        } else {
            throw new UnsupportedDataTypeException();
        }
    }
}
