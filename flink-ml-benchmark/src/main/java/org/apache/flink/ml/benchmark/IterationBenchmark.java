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
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.ml.common.iteration.ForwardInputsOfLastRound;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
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

    private static List<Long> executeAndCollect(StreamExecutionEnvironment env, int maxIter)
            throws Exception {
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

        return IteratorUtils.toList(output.executeAndCollect());
    }

    private static void executeWithArgs(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(OPTIONS, args);

        int maxIter = Integer.parseInt(commandLine.getOptionValue(ITER_OPTION));
        int repeat = Integer.parseInt(commandLine.getOptionValue(REPEAT_OPTION));

        System.out.println("iteration: " + maxIter + " repeat: " + repeat);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(10);

        for (int i = 0; i < repeat; i++) {
            long startTime = System.currentTimeMillis();
            System.out.println(executeAndCollect(env, maxIter));
            System.out.println(System.currentTimeMillis() - startTime);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            executeWithArgs(args);
        }

        List<Integer> iterList = Arrays.asList(10, 20, 50, 100, 200, 500);
        List<Integer> timeoutList = Arrays.asList(0, 1, 10, 100);

        for (int bufferTimeout : timeoutList) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setBufferTimeout(bufferTimeout);

            for (int maxIter : iterList) {
                long startTime = System.currentTimeMillis();
                executeAndCollect(env, maxIter);
                System.out.printf(
                        "Timeout: %d ms\t MaxIter: %d \t Time: %d ms\n",
                        bufferTimeout, maxIter, System.currentTimeMillis() - startTime);
            }
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

            List<DataStream<Long>> variableList = new ArrayList<>();
            List<DataStream<?>> dataList = new ArrayList<>();
            for (int i = 0; i < variableStreams.size(); i++) {
                DataStream<Long> stream = variableStreams.get(i);
                stream =
                        stream.transform(
                                "AddOneOperator",
                                BasicTypeInfo.LONG_TYPE_INFO,
                                new AddOneOperator());
                variableList.add(stream);
                dataList.add(stream.flatMap(new ForwardInputsOfLastRound<>()));
            }

            return new IterationBodyResult(
                    DataStreamList.of(variableList.toArray(new DataStream<?>[0])),
                    DataStreamList.of(dataList.toArray(new DataStream<?>[0])),
                    terminationCriteria);
        }
    }

    private static class AddOneOperator extends AbstractStreamOperator<Long>
            implements OneInputStreamOperator<Long, Long>, IterationListener<Long> {
        private final List<Long> list = new ArrayList<>();

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<Long> collector) {
            for (Long x : list) {
                collector.collect(x + 1);
            }
            list.clear();
        }

        @Override
        public void onIterationTerminated(Context context, Collector<Long> collector) {}

        @Override
        public void processElement(StreamRecord<Long> streamRecord) {
            list.add(streamRecord.getValue());
        }
    }
}
