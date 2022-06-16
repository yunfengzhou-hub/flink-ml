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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.IteratorUtils;

import java.util.ArrayList;
import java.util.List;

/** Example code to test the performance of flink-ml-iteration. */
public class IterationBenchmark {
    private static final Option ITER_OPTION =
            Option.builder("i")
                    .longOpt("iteration")
                    .desc("Number of iterations to be executed.")
                    .required()
                    .hasArg()
                    .build();

    private static final Option REPEAT_OPTION =
            Option.builder("r")
                    .longOpt("repeat")
                    .desc("Times to repeat the execution.")
                    .required()
                    .hasArg()
                    .build();

    private static final Options OPTIONS =
            new Options().addOption(ITER_OPTION).addOption(REPEAT_OPTION);

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(OPTIONS, args);

        int maxIter = Integer.parseInt(commandLine.getOptionValue(ITER_OPTION));
        int repeat = Integer.parseInt(commandLine.getOptionValue(REPEAT_OPTION));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        for (int i = 0; i < repeat; i++) {
            DataStream<Long> stream =
                    env.fromParallelCollection(
                            new NumberSequenceIterator(1L, 10L), BasicTypeInfo.LONG_TYPE_INFO);

            IterationConfig config =
                    IterationConfig.newBuilder()
                            .setOperatorLifeCycle(IterationConfig.OperatorLifeCycle.ALL_ROUND)
                            .build();

            IterationBody iterationBody = new NoOpIterationBody(maxIter);

            DataStream<Long> output =
                    Iterations.iterateBoundedStreamsUntilTermination(
                                    DataStreamList.of(stream),
                                    ReplayableDataStreamList.notReplay(stream),
                                    config,
                                    iterationBody)
                            .get(0);

            long startTime = System.currentTimeMillis();
            if (IteratorUtils.toList(output.executeAndCollect()).size() != 10L) {
                throw new RuntimeException();
            }
            System.out.println(System.currentTimeMillis() - startTime);
        }
    }

    private static class NoOpIterationBody implements IterationBody {
        private final int maxIterationNum;

        private NoOpIterationBody(int maxIterationNum) {
            this.maxIterationNum = maxIterationNum;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<Integer> terminationCriteria =
                    variableStreams.get(0).flatMap(new TerminateOnMaxIter<>(maxIterationNum));

            List<DataStream<?>> variableList = new ArrayList<>();
            for (DataStream<?> stream : variableStreams.getDataStreams()) {
                variableList.add(stream.map(x -> x));
            }

            List<DataStream<?>> dataList = new ArrayList<>();
            for (DataStream<?> stream : dataStreams.getDataStreams()) {
                dataList.add(stream.map(x -> x));
            }

            return new IterationBodyResult(
                    DataStreamList.of(variableList.toArray(new DataStream<?>[0])),
                    DataStreamList.of(dataList.toArray(new DataStream<?>[0])),
                    terminationCriteria);
        }
    }
}
