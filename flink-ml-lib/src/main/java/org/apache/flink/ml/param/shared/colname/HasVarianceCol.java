package org.apache.flink.ml.param.shared.colname;

import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.StringParam;
import org.apache.flink.ml.param.WithParams;

public interface HasVarianceCol<T> extends WithParams<T> {
    Param<String> VARIANCE_COL =
            new StringParam(
                    "varianceCol",
                    "Column name for the biased sample variance of prediction.",
                    null);

    default String getVarianceCol() {
        return get(VARIANCE_COL);
    }

    default T setVarianceCol(String value) {
        return set(VARIANCE_COL, value);
    }
}