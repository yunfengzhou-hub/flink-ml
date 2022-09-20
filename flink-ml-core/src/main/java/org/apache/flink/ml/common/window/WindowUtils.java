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
            Time gap = Time.of(
                    ((SessionWindow) window).gap.getSize(),
                    ((SessionWindow) window).gap.getUnit()
            );
            if (((SessionWindow) window).isEventTime) {
                return EventTimeSessionWindows.withGap(gap);
            } else {
                return ProcessingTimeSessionWindows.withGap(gap);
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported %s subclass: %s", Window.class, window.getClass()));
        }
    }
}
