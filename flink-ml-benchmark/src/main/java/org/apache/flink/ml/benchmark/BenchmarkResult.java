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

import org.apache.flink.util.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;

/** The result of executing a benchmark. */
public class BenchmarkResult {
    /** The benchmark name. */
    public final String name;

    /** The total execution time of the benchmark in milliseconds. */
    public final Double totalTimeMs;

    /** The total number of input records. */
    public final Long inputRecordNum;

    /** The average input throughput in number of records per second. */
    public final Double inputThroughput;

    /** The total number of output records. */
    public final Long outputRecordNum;

    /** The average output throughput in number of records per second. */
    public final Double outputThroughput;

    private BenchmarkResult(
            String name,
            Double totalTimeMs,
            Long inputRecordNum,
            Double inputThroughput,
            Long outputRecordNum,
            Double outputThroughput) {
        Preconditions.checkNotNull(name);
        this.name = name;
        this.totalTimeMs = totalTimeMs;
        this.inputRecordNum = inputRecordNum;
        this.inputThroughput = inputThroughput;
        this.outputRecordNum = outputRecordNum;
        this.outputThroughput = outputThroughput;
    }

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

    /**
     * A nested builder class to create {@link BenchmarkResult} instances using descriptive methods.
     */
    public static class Builder {
        private String name;
        private Double totalTimeMs;
        private Long inputRecordNum;
        private Double inputThroughput;
        private Long outputRecordNum;
        private Double outputThroughput;

        public BenchmarkResult build() {
            return new BenchmarkResult(
                    name,
                    totalTimeMs,
                    inputRecordNum,
                    inputThroughput,
                    outputRecordNum,
                    outputThroughput);
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setTotalTimeMs(Double totalTimeMs) {
            this.totalTimeMs = totalTimeMs;
            return this;
        }

        public Builder setInputRecordNum(Long inputRecordNum) {
            this.inputRecordNum = inputRecordNum;
            return this;
        }

        public Builder setInputThroughput(Double inputThroughput) {
            this.inputThroughput = inputThroughput;
            return this;
        }

        public Builder setOutputRecordNum(Long outputRecordNum) {
            this.outputRecordNum = outputRecordNum;
            return this;
        }

        public Builder setOutputThroughput(Double outputThroughput) {
            this.outputThroughput = outputThroughput;
            return this;
        }
    }
}
