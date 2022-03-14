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

import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Entry class for benchmark execution. */
public class Benchmark {
    private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

    static final String VERSION_KEY = "version";

    static final String SHELL_SCRIPT = "flink-ml-benchmark.sh";

    static final Option HELP_OPTION =
            Option.builder("h")
                    .longOpt("help")
                    .desc("Show the help message for the command line interface.")
                    .build();

    static final Option OUTPUT_FILE_OPTION =
            Option.builder()
                    .longOpt("output-file")
                    .desc("The output file name to save benchmark results.")
                    .hasArg()
                    .build();

    static final Options OPTIONS =
            new Options().addOption(HELP_OPTION).addOption(OUTPUT_FILE_OPTION);

    public static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);

        System.out.println("./" + SHELL_SCRIPT + " <config-file-path> [OPTIONS]");
        System.out.println();
        formatter.setSyntaxPrefix("The following options are available:");
        formatter.printHelp(" ", OPTIONS);

        System.out.println();
    }

    @SuppressWarnings("unchecked")
    public static void executeBenchmarks(CommandLine commandLine) throws Exception {
        String configFile = commandLine.getArgs()[0];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        InputStream inputStream = new FileInputStream(configFile);
        Map<String, ?> jsonMap = ReadWriteUtils.OBJECT_MAPPER.readValue(inputStream, Map.class);
        Preconditions.checkArgument(
                jsonMap.containsKey(VERSION_KEY) && jsonMap.get(VERSION_KEY).equals(1));

        List<String> benchmarkNames =
                jsonMap.keySet().stream()
                        .filter(x -> !x.equals(VERSION_KEY))
                        .collect(Collectors.toList());
        LOG.info("Found benchmarks " + benchmarkNames);

        List<BenchmarkResult> results = new ArrayList<>();

        for (String benchmarkName : benchmarkNames) {
            LOG.info("Running benchmark " + benchmarkName + ".");

            BenchmarkResult result =
                    BenchmarkUtils.runBenchmark(
                            tEnv, benchmarkName, (Map<String, ?>) jsonMap.get(benchmarkName));

            results.add(result);
            BenchmarkUtils.printResults(result);
        }

        if (commandLine.hasOption(OUTPUT_FILE_OPTION.getLongOpt())) {
            String saveFile = commandLine.getOptionValue(OUTPUT_FILE_OPTION.getLongOpt());
            BenchmarkUtils.saveResultsAsJson(saveFile, results.toArray(new BenchmarkResult[0]));
            LOG.info("Benchmark results saved as json in " + saveFile + ".");
        }
    }

    public static void printInvalidError(String[] args) {
        System.out.println("Invalid command line arguments " + Arrays.toString(args));
        System.out.println();
        System.out.println("Specify the help option (-h or --help) to get help on the command.");
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(OPTIONS, args);

        if (commandLine.hasOption(HELP_OPTION.getLongOpt())) {
            printHelp();
        } else if (commandLine.getArgs().length == 1) {
            executeBenchmarks(commandLine);
        } else {
            printInvalidError(args);
        }
    }
}