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

package org.apache.flink.ml.benchmark;

import java.util.LinkedHashMap;
import java.util.Map;

/** The result of executing a benchmark. */
public class BenchmarkResult {
    /** The name of the benchmark. */
    public String name;

    /** The total execution time of the benchmark flink job. Unit: milliseconds */
    public Double totalTimeMs;

    /** The total number of records input into the benchmark flink job. */
    public Long inputRecordNum;

    /**
     * The average input throughput of the benchmark flink job. Unit: number of records processed
     * per second
     */
    public Double inputThroughput;

    /** The total number of records output from the benchmark flink job. */
    public Long outputRecordNum;

    /**
     * The average output throughput of the benchmark flink job. Unit: number of records processed
     * per second
     */
    public Double outputThroughput;

    /** Converts the object to a Map. */
    public Map<String, ?> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", name);
        map.put("totalTimeMs", totalTimeMs);
        map.put("inputRecordNum", inputRecordNum);
        map.put("inputThroughput", inputThroughput);
        map.put("outputRecordNum", outputRecordNum);
        map.put("outputThroughput", outputThroughput);
        return map;
    }
}
