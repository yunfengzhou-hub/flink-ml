package org.apache.flink.ml.common.param;


import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.StringArrayParam;
import org.apache.flink.ml.param.WithParams;

public interface HasInputCols<T> extends WithParams<T> {
    Param<String[]> INPUT_COLS =
            new StringArrayParam(
                    "inputCols",
                    "Input column names.",
                    new String[]{"input"},
                    ParamValidators.lenGt(0));

    default String[] getInputCols() {
        return get(INPUT_COLS);
    }

    default T setInputCols(String... value) {
        return set(INPUT_COLS, value);
    }
}
