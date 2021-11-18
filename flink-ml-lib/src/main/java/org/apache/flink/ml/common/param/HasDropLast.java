package org.apache.flink.ml.common.param;

import org.apache.flink.ml.param.*;

public interface HasDropLast<T> extends WithParams<T> {
    Param<Boolean> DROP_LAST =
            new BooleanParam(
                    "dropLast",
                    "Whether to drop the last category.",
                    true);

    default boolean getDropLast() {
        return get(DROP_LAST);
    }

    default T setDropLast(boolean value) {
        return set(DROP_LAST, value);
    }
}
