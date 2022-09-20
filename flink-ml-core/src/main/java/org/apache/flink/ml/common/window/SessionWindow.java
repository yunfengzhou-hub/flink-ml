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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Window} that windows elements into sessions based on the timestamp of the elements.
 * Windows do not overlap.
 */
public class SessionWindow implements Window {
    /** The session timeout, i.e. the time gap between sessions. */
    Time gap;

    boolean isEventTime;

    private SessionWindow() {
        this.gap = null;
        this.isEventTime = true;
    }

    /**
     * Creates a new {@link SessionWindow}.
     *
     * @param gap The session timeout, i.e. the time gap between sessions
     */
    public static SessionWindow withGap(Time gap) {
        SessionWindow sessionWindow = new SessionWindow();
        sessionWindow.gap = gap;
        return sessionWindow;
    }

    public SessionWindow withEventTime() {
        isEventTime = true;
        return this;
    }

    public SessionWindow withProcessingTime() {
        isEventTime = false;
        return this;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("class", this.getClass().getName());
        if (gap != null) {
            map.put("gapSize", gap.getSize());
            map.put("gapUnit", gap.getUnit().toString());
        }
        map.put("isEventTime", isEventTime);
        return map;
    }

    public static SessionWindow parse(Map<String, Object> map) {
        Preconditions.checkArgument(SessionWindow.class.getName().equals(map.get("class")));
        Time gap = null;
        if (map.containsKey("gapSize")) {
            long size = (long) map.get("gapSize");
            TimeUnit unit = TimeUnit.valueOf((String) map.get("gapUnit"));
            gap = Time.of(size, unit);
        }
        boolean isEventTime = (boolean) map.get("isEventTime");

        SessionWindow sessionWindow = SessionWindow.withGap(gap);
        sessionWindow.isEventTime = isEventTime;

        return sessionWindow;
    }

    @Override
    public int hashCode() {
        return Objects.hash(gap, isEventTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SessionWindow)) {
            return false;
        }

        SessionWindow window = (SessionWindow) obj;

        boolean isEqual = this.isEventTime == window.isEventTime;
        isEqual &= Objects.equals(this.gap, window.gap);

        return isEqual;
    }
}
