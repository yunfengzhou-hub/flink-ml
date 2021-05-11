package org.apache.flink.ml.common.function.environment;

import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;

import java.io.File;

/** Helper methods to easily create a {@link LocalRecoveryConfig} for tests. */
public class TestLocalRecoveryConfig {

    private static final LocalRecoveryDirectoryProvider INSTANCE =
            new TestDummyLocalDirectoryProvider();

    public static LocalRecoveryConfig disabled() {
        return new LocalRecoveryConfig(false, INSTANCE);
    }

    public static class TestDummyLocalDirectoryProvider implements LocalRecoveryDirectoryProvider {

        private TestDummyLocalDirectoryProvider() {}

        @Override
        public File allocationBaseDirectory(long checkpointId) {
            throw new UnsupportedOperationException("Test dummy");
        }

        @Override
        public File subtaskBaseDirectory(long checkpointId) {
            throw new UnsupportedOperationException("Test dummy");
        }

        @Override
        public File subtaskSpecificCheckpointDirectory(long checkpointId) {
            throw new UnsupportedOperationException("Test dummy");
        }

        @Override
        public File selectAllocationBaseDirectory(int idx) {
            throw new UnsupportedOperationException("Test dummy");
        }

        @Override
        public File selectSubtaskBaseDirectory(int idx) {
            throw new UnsupportedOperationException("Test dummy");
        }

        @Override
        public int allocationBaseDirsCount() {
            throw new UnsupportedOperationException("Test dummy");
        }
    }
}