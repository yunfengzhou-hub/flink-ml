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

import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/** A {@link Window} that windows all elements in a bounded stream into one window. */
public class BoundedWindow implements Window {
    private static final BoundedWindow INSTANCE = new BoundedWindow();

    private BoundedWindow() {}

    public static BoundedWindow get() {
        return INSTANCE;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("class", this.getClass().getName());
        return map;
    }

    public static BoundedWindow parse(Map<String, Object> map) {
        Preconditions.checkArgument(BoundedWindow.class.getName().equals(map.get("class")));
        return INSTANCE;
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BoundedWindow;
    }
}
