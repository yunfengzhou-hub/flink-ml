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

import java.time.Duration;

/**
 * A {@link Window} that windows elements into fixed-size windows based on the timestamp of the
 * elements. Windows do not overlap.
 */
public class TumbleWindow implements Window {
    String timestampCol;
    Duration timeWindowSize;
    Duration timeWindowOffset;
    long countWindowSize;

    private TumbleWindow() {
        this.timestampCol = null;
        this.timeWindowSize = null;
        this.timeWindowOffset = Duration.ZERO;
        this.countWindowSize = -1;
    }

    public static TumbleWindow over(Duration size) {
        TumbleWindow tumbleWindow = new TumbleWindow();
        tumbleWindow.timeWindowSize = size;
        return tumbleWindow;
    }

    public static TumbleWindow over(long size) {
        TumbleWindow tumbleWindow = new TumbleWindow();
        tumbleWindow.countWindowSize = size;
        return tumbleWindow;
    }

    public TumbleWindow on(String timestampCol) {
        this.timestampCol = timestampCol;
        return this;
    }
}
