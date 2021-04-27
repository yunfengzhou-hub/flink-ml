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

package org.apache.flink.ml.common.function.environment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

@Internal
public class EmbedRuntimeEnvironment implements Environment {

    private final JobID jobId = new JobID();
    private final JobVertexID jobVertexId = new JobVertexID();
    private final ExecutionAttemptID executionId = new ExecutionAttemptID();
    private final TaskInfo taskInfo;
    private final AccumulatorRegistry accumulatorRegistry =
            new AccumulatorRegistry(jobId, executionId);

    public EmbedRuntimeEnvironment() {
        this("Function Job", 1, 0, 1);
    }

    public EmbedRuntimeEnvironment(
            String taskName, int numSubTasks, int subTaskIndex, int maxParallelism) {
        this.taskInfo = new TaskInfo(taskName, maxParallelism, subTaskIndex, numSubTasks, 0);
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
        return null;
    }

    @Override
    public JobID getJobID() {
        return jobId;
    }

    @Override
    public JobVertexID getJobVertexId() {
        return jobVertexId;
    }

    @Override
    public ExecutionAttemptID getExecutionId() {
        return executionId;
    }

    @Override
    public Configuration getTaskConfiguration() {
        return new Configuration();
    }

    @Override
    public TaskManagerRuntimeInfo getTaskManagerInfo() {
        return new EmbedTaskManagerRuntimeInfo();
    }

    @Override
    public TaskMetricGroup getMetricGroup() {
        return UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
    }

    @Override
    public Configuration getJobConfiguration() {
        return new Configuration();
    }

    @Override
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    @Override
    public InputSplitProvider getInputSplitProvider() {
        return null;
    }

    @Override
    public IOManager getIOManager() {
        return null;
    }

    @Override
    public MemoryManager getMemoryManager() {
        return null;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return EmbedUserCodeClassLoader.newBuilder().build();
    }

    @Override
    public Map<String, Future<Path>> getDistributedCacheEntries() {
        return Collections.emptyMap();
    }

    @Override
    public BroadcastVariableManager getBroadcastVariableManager() {
        return null;
    }

    @Override
    public TaskStateManager getTaskStateManager() {
        return null;
    }

    @Override
    public GlobalAggregateManager getGlobalAggregateManager() {
        return null;
    }

    @Override
    public AccumulatorRegistry getAccumulatorRegistry() {
        return accumulatorRegistry;
    }

    @Override
    public TaskKvStateRegistry getTaskKvStateRegistry() {
        return null;
    }

    @Override
    public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExternalResourceInfoProvider getExternalResourceInfoProvider() {
        return ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES;
    }

    @Override
    public void acknowledgeCheckpoint(
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot subtaskState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void declineCheckpoint(long l, Throwable throwable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void failExternally(Throwable cause) {
        throw new UnsupportedOperationException(
                String.format("%s does not support external task failure.", this.getClass()));
    }

    @Override
    public ResultPartitionWriter getWriter(int index) {
        return null;
    }

    @Override
    public ResultPartitionWriter[] getAllWriters() {
        return new ResultPartitionWriter[0];
    }

    @Override
    public IndexedInputGate getInputGate(int index) {
        throw new ArrayIndexOutOfBoundsException(0);
    }

    @Override
    public IndexedInputGate[] getAllInputGates() {
        return new IndexedInputGate[0];
    }

    @Override
    public TaskEventDispatcher getTaskEventDispatcher() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskOperatorEventGateway getOperatorCoordinatorEventGateway() {
        return new NoOpTaskOperatorEventGateway();
    }

    private static final class NoOpTaskOperatorEventGateway implements TaskOperatorEventGateway {
        @Override
        public void sendOperatorEventToCoordinator(
                OperatorID operator, SerializedValue<OperatorEvent> event) {}
    }
}
