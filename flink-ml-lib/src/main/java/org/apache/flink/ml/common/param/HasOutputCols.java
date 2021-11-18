package org.apache.flink.ml.common.param;

import org.apache.flink.ml.param.*;

public interface HasOutputCols<T> extends WithParams<T> {
    Param<String[]> OUTPUT_COLS =
            new StringArrayParam(
                    "outputCols",
                    "Output column names.",
                    new String[]{"output"},
                    ParamValidators.lenGt(0));

    default String[] getOutputCols() {
        return get(OUTPUT_COLS);
    }

    default T setOutputCols(String... value) {
        return set(OUTPUT_COLS, value);
    }
}
