package org.apache.flink.ml.common.function;

import java.util.List;

public interface StreamFunction<T, R> {
    List<R> apply(T t);
}
