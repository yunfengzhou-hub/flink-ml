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

package org.apache.flink.iteration.operator;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.datacache.nonkeyed.DataCache;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheIterator;
import org.apache.flink.iteration.progresstrack.OperatorEpochWatermarkTracker;
import org.apache.flink.iteration.progresstrack.OperatorEpochWatermarkTrackerFactory;
import org.apache.flink.iteration.progresstrack.OperatorEpochWatermarkTrackerListener;
import org.apache.flink.iteration.typeinfo.IterationRecordSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.util.NonClosingInputStreamDecorator;
import org.apache.flink.runtime.util.NonClosingOutputStreamDecorator;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.collections.IteratorUtils;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** Replays the data received in the round 0 in the following round. */
public class ReplayOperator<T> extends AbstractStreamOperator<IterationRecord<T>>
        implements TwoInputStreamOperator<
                        IterationRecord<T>, IterationRecord<Void>, IterationRecord<T>>,
                OperatorEpochWatermarkTrackerListener,
                BoundedMultiInput {

    private OperatorEpochWatermarkTracker progressTracker;

    private Path basePath;

    private FileSystem fileSystem;

    private TypeSerializer<T> typeSerializer;

    private MailboxExecutor mailboxExecutor;

    private DataCache<T> dataCache;

    @Nullable private DataCacheIterator<T> currentDataCacheIterator;

    private int currentEpoch;

    // ------------- states -------------------

    /** Stores the parallelism of the last run so that we could check if parallelism is changed. */
    private ListState<Integer> parallelismState;

    /** Stores the current epoch. */
    private ListState<Integer> currentEpochState;

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<IterationRecord<T>>> output) {
        super.setup(containingTask, config, output);
        progressTracker = OperatorEpochWatermarkTrackerFactory.create(config, containingTask, this);

        try {
            basePath =
                    OperatorUtils.getDataCachePath(
                            containingTask.getEnvironment().getTaskManagerInfo().getConfiguration(),
                            containingTask
                                    .getEnvironment()
                                    .getIOManager()
                                    .getSpillingDirectoriesPaths());
            fileSystem = basePath.getFileSystem();

            IterationRecordSerializer<T> iterationRecordSerializer =
                    (IterationRecordSerializer)
                            config.getTypeSerializerOut(getClass().getClassLoader());
            typeSerializer = iterationRecordSerializer.getInnerSerializer();

            mailboxExecutor =
                    containingTask
                            .getMailboxExecutorFactory()
                            .createExecutor(TaskMailbox.MIN_PRIORITY);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        parallelismState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>("parallelism", IntSerializer.INSTANCE));
        OperatorStateUtils.getUniqueElement(parallelismState, "parallelism")
                .ifPresent(
                        oldParallelism ->
                                checkState(
                                        oldParallelism
                                                == getRuntimeContext()
                                                        .getNumberOfParallelSubtasks(),
                                        "The Replay operator is recovered with parallelism changed from "
                                                + oldParallelism
                                                + " to "
                                                + getRuntimeContext()
                                                        .getNumberOfParallelSubtasks()));

        currentEpochState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<Integer>("epoch", IntSerializer.INSTANCE));
        OperatorStateUtils.getUniqueElement(currentEpochState, "epoch")
                .ifPresent(epoch -> currentEpoch = epoch);

        try {
            SupplierWithException<Path, IOException> pathGenerator =
                    OperatorUtils.createDataCacheFileGenerator(
                            basePath, "replay", config.getOperatorID());

            List<StatePartitionStreamProvider> rawStateInputs =
                    IteratorUtils.toList(context.getRawOperatorStateInputs().iterator());
            if (rawStateInputs.size() > 0) {
                checkState(
                        rawStateInputs.size() == 1,
                        "Currently the replay operator does not support rescaling");

                dataCache =
                        DataCache.recover(
                                rawStateInputs.get(0).getStream(),
                                typeSerializer,
                                fileSystem,
                                pathGenerator);

                try (DataInputStream dis =
                        new DataInputStream(
                                new NonClosingInputStreamDecorator(
                                        rawStateInputs.get(0).getStream()))) {
                    boolean hasReader = dis.readBoolean();
                    if (hasReader) {
                        currentDataCacheIterator = dataCache.iterator();
                        currentDataCacheIterator.setPos(dis.readInt());
                    }
                }
            } else {
                dataCache = new DataCache<>(typeSerializer, fileSystem, pathGenerator);
            }

        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to replay the records", e);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        // Always clears the union list state before set value.
        parallelismState.clear();
        if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
            parallelismState.update(
                    Collections.singletonList(getRuntimeContext().getNumberOfParallelSubtasks()));
        }

        currentEpochState.update(Collections.singletonList(currentEpoch));

        context.getRawOperatorStateOutput().startNewPartition();
        dataCache.writeTo(context.getRawOperatorStateOutput());
        try (DataOutputStream dos =
                new DataOutputStream(
                        new NonClosingOutputStreamDecorator(context.getRawOperatorStateOutput()))) {
            dos.writeBoolean(currentDataCacheIterator != null);
            if (currentDataCacheIterator != null) {
                dos.writeInt(currentDataCacheIterator.getPos());
            }
        }
    }

    @Override
    public void processElement1(StreamRecord<IterationRecord<T>> element) throws Exception {
        switch (element.getValue().getType()) {
            case RECORD:
                dataCache.addRecord(element.getValue().getValue());
                output.collect(element);
                break;
            case EPOCH_WATERMARK:
                progressTracker.onEpochWatermark(
                        0, element.getValue().getSender(), element.getValue().getEpoch());
                break;
            default:
                throw new UnsupportedOperationException(
                        "Not supported element type: " + element.getValue());
        }
    }

    @Override
    public void processElement2(StreamRecord<IterationRecord<Void>> element) throws Exception {
        if (element.getValue().getType() == IterationRecord.Type.EPOCH_WATERMARK) {
            progressTracker.onEpochWatermark(
                    1, element.getValue().getSender(), element.getValue().getEpoch());
        } else {
            throw new UnsupportedOperationException(
                    "Not supported element type: " + element.getValue());
        }
    }

    @Override
    public void endInput(int i) throws Exception {
        // The notification ranges from 1 to N while the track uses 0 to N -1.
        progressTracker.finish(i - 1);

        // 1. If in the last checkpoint the epoch 0 is not finished, currentDataCacheReader must be
        // null.
        // 2. If in the last checkpoint the epoch 0 is finished and currentDataCacheReader is not
        // null, then after failover we need to emit all the following records.
        if (i == 1 && currentDataCacheIterator != null) {
            // We have to finish emit the following records.
            replayRecords(currentDataCacheIterator, currentEpoch);
        }
    }

    @Override
    public void onEpochWatermarkIncrement(int epochWatermark) throws IOException {
        if (epochWatermark == 0) {
            // No need to replay for the round 0, it is output directly.
            // TODO: free cached records when they will no longer be replayed, and enable caching
            // data in memory.
            dataCache.finish();
            emitEpochWatermark(epochWatermark);
            return;
        } else if (epochWatermark == Integer.MAX_VALUE) {
            emitEpochWatermark(epochWatermark);
            return;
        }

        // At this point, there would be no more inputs before we finish replaying all the data.
        // Thus it is safe for us to implement our own mailbox loop.
        checkState(currentDataCacheIterator == null, "Concurrent replay is not supported");
        currentEpoch = epochWatermark;
        currentDataCacheIterator = dataCache.iterator();
        replayRecords(currentDataCacheIterator, epochWatermark);
    }

    private void replayRecords(DataCacheIterator<T> dataCacheIterator, int epoch) {
        StreamRecord<IterationRecord<T>> reusable =
                new StreamRecord<>(IterationRecord.newRecord(null, epoch));
        while (dataCacheIterator.hasNext()) {
            // we first process the pending mail
            while (mailboxExecutor.tryYield()) {
                // Do nothing.
            }

            T next = dataCacheIterator.next();
            reusable.getValue().setValue(next);
            output.collect(reusable);
        }

        currentDataCacheIterator = null;

        emitEpochWatermark(epoch);
    }

    private void emitEpochWatermark(int epoch) {
        output.collect(
                new StreamRecord<>(
                        IterationRecord.newEpochWatermark(
                                epoch,
                                OperatorUtils.getUniqueSenderId(
                                        config.getOperatorID(),
                                        getContainingTask().getIndexInSubtaskGroup()))));
    }
}
