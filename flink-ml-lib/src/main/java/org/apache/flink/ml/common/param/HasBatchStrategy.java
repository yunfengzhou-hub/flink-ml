package org.apache.flink.ml.common.param;

import org.apache.flink.ml.param.*;

/** Interface for the shared batch strategy param. */
@SuppressWarnings("unchecked")
public interface HasBatchStrategy<T> extends WithParams<T> {
    String COUNT_STRATEGY = "count";

    Param<String> BATCH_STRATEGY =
            new StringParam(
                    "batchStrategy",
                    "Strategy to create mini batch from online train data.",
                    COUNT_STRATEGY,
                    ParamValidators.inArray(COUNT_STRATEGY));

    Param<Integer> BATCH_SIZE =
            new IntParam(
                    "batchSize",
                    "Number of elements in a batch.",
                    1,
                    ParamValidators.gt(0));

    default String getBatchStrategy() {
        return get(BATCH_STRATEGY);
    }

    default T setBatchSize(int value) {
        set(BATCH_STRATEGY, COUNT_STRATEGY);
        set(BATCH_SIZE, value);
        return (T) this;
    }

    default int getBatchSize() {
        return get(BATCH_SIZE);
    }
}
