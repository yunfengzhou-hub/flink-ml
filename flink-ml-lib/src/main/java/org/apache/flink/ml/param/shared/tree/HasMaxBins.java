package org.apache.flink.ml.param.shared.tree;

import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.WithParams;

public interface HasMaxBins<T> extends WithParams<T> {
    Param<Integer> MAX_BINS =
            new IntParam(
                    "maxBins",
                    "MAX number of bins for continuous feature",
                    128);

    default Integer getMaxBins() {
        return get(MAX_BINS);
    }

    default T setMaxBins(Integer value) {
        return set(MAX_BINS, value);
    }
}