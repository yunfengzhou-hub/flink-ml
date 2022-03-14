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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/** The result of executing a {@link Benchmark}. */
public class BenchmarkResult {
    private long executionTime;
    private final Benchmark<?> benchmark;

    public BenchmarkResult(Benchmark<?> benchmark) {
        this.benchmark = benchmark;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public BenchmarkResult setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
        return this;
    }

    public static void save(String fileName, BenchmarkResult... results) throws IOException {
        FileWriter fileWriter = new FileWriter(fileName);
        PrintWriter printWriter = new PrintWriter(fileWriter);

        printWriter.println("This file contains " + results.length + " benchmark results.");

        for (BenchmarkResult result : results) {
            printWriter.println();
            printWriter.println("Benchmark " + result.benchmark.getName() + ":");
            printWriter.println("Execution Time: " + result.getExecutionTime());
        }

        printWriter.println();
        printWriter.close();
    }
}
