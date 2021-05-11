package org.apache.flink.ml.common.function.environment;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;

import java.io.IOException;

public class TestGlobalAggregateManager implements GlobalAggregateManager {

    @Override
    public <IN, ACC, OUT> OUT updateGlobalAggregate(
            String aggregateName,
            Object aggregand,
            AggregateFunction<IN, ACC, OUT> aggregateFunction)
            throws IOException {
        return aggregateFunction.getResult(aggregateFunction.createAccumulator());
    }
}
