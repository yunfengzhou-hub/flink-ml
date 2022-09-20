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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Window} that windows elements into fixed-size windows based on the timestamp of the
 * elements. Windows do not overlap.
 */
public class TumbleWindow implements Window {
    /** Size of this window as time interval */
    Time timeWindowSize;

    /** Offset of this window. Windows start at time N * size + offset, where 0 is the epoch. */
    Time timeWindowOffset;

    /** Size of this window as row-count interval. */
    long countWindowSize;

    boolean isEventTime;

    private TumbleWindow() {
        this.timeWindowSize = null;
        this.timeWindowOffset = null;
        this.isEventTime = true;
        this.countWindowSize = -1;
    }

    /**
     * Creates a new {@link TumbleWindow}.
     *
     * @param size the size of the window as time interval.
     */
    public static TumbleWindow over(Time size) {
        return TumbleWindow.over(size, Time.milliseconds(0));
    }

    /**
     * Creates a new {@link TumbleWindow}.
     *
     * @param size the size of the window as time interval.
     * @param offset the offset of this window.
     */
    public static TumbleWindow over(Time size, Time offset) {
        TumbleWindow tumbleWindow = new TumbleWindow();
        tumbleWindow.timeWindowSize = size;
        tumbleWindow.timeWindowOffset = offset;
        return tumbleWindow;
    }

    /**
     * Creates a new {@link TumbleWindow}.
     *
     * @param size the size of the window as row-count interval.
     */
    public static TumbleWindow over(long size) {
        TumbleWindow tumbleWindow = new TumbleWindow();
        tumbleWindow.countWindowSize = size;
        return tumbleWindow;
    }

    public TumbleWindow withEventTime() {
        isEventTime = true;
        return this;
    }

    public TumbleWindow withProcessingTime() {
        isEventTime = false;
        return this;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("class", this.getClass().getName());
        if (timeWindowSize != null) {
            map.put("timeWindowSizeSize", timeWindowSize.getSize());
            map.put("timeWindowSizeUnit", timeWindowSize.getUnit());
        }
        if (timeWindowOffset != null) {
            map.put("timeWindowOffsetSize", timeWindowOffset.getSize());
            map.put("timeWindowOffsetUnit", timeWindowOffset.getUnit());
        }
        map.put("isEventTime", isEventTime);
        map.put("countWindowSize", countWindowSize);
        return map;
    }

    public static TumbleWindow parse(Map<String, Object> map) {
        Preconditions.checkArgument(TumbleWindow.class.getName().equals(map.get("class")));
        Time timeWindowSize = null;
        if (map.containsKey("timeWindowSizeSize")) {
            long size = (long) map.get("timeWindowSizeSize");
            TimeUnit unit = TimeUnit.valueOf((String) map.get("timeWindowSizeUnit"));
            timeWindowSize = Time.of(size, unit);
        }
        Time timeWindowOffset = null;
        if (map.containsKey("timeWindowOffsetSize")) {
            long size = (long) map.get("timeWindowOffsetSize");
            TimeUnit unit = TimeUnit.valueOf((String) map.get("timeWindowOffsetUnit"));
            timeWindowOffset = Time.of(size, unit);
        }
        long countWindowSize = ((Number) map.get("countWindowSize")).longValue();
        boolean isEventTime = (boolean) map.get("isEventTime");

        TumbleWindow tumbleWindow = TumbleWindow.over(countWindowSize);
        tumbleWindow.timeWindowOffset = timeWindowOffset;
        tumbleWindow.timeWindowSize = timeWindowSize;
        tumbleWindow.isEventTime = isEventTime;

        return tumbleWindow;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeWindowSize, timeWindowOffset, isEventTime, countWindowSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TumbleWindow)) {
            return false;
        }

        TumbleWindow window = (TumbleWindow) obj;

        boolean isEqual = this.isEventTime == window.isEventTime;
        isEqual &= this.countWindowSize == window.countWindowSize;
        isEqual &= Objects.equals(this.timeWindowSize, window.timeWindowSize);
        isEqual &= Objects.equals(this.timeWindowOffset, window.timeWindowOffset);

        return isEqual;
    }
}
