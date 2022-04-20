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

package org.apache.flink.ml.param;

import java.io.IOException;

/** Class for the integer parameter. */
public class IntParam extends Param<Integer> {

    public IntParam(
            String name,
            String description,
            Integer defaultValue,
            ParamValidator<Integer> validator) {
        super(name, Integer.class, description, defaultValue, validator);
    }

    public IntParam(String name, String description, Integer defaultValue) {
        this(name, description, defaultValue, ParamValidators.alwaysTrue());
    }

    @Override
    public Integer jsonDecode(Object json) throws IOException {
        if (json == null) {
            return null;
        } else if (json instanceof Number) {
            return ((Number) json).intValue();
        } else {
            throw new IOException("Cannot convert json " + json + " to Integer.");
        }
    }
}
