package org.apache.flink.ml.feature.onehotencoder;

import org.apache.flink.ml.common.param.HasDropLast;
import org.apache.flink.ml.common.param.HasInputCols;
import org.apache.flink.ml.common.param.HasOutputCols;

public interface OneHotEncoderModelParams<T>
    extends HasInputCols<T>, HasOutputCols<T>, HasDropLast<T> {
}
