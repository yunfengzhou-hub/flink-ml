package org.apache.flink.ml.param.shared.tree;

import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.WithParams;

public interface HasMaxDepth<T> extends WithParams<T> {
    Param<Integer> MAX_DEPTH =
            new IntParam(
                    "maxDepth",
                    "Depth of the tree.",
                    Integer.MAX_VALUE);

    default int getHasMaxDepth() {
        return get(MAX_DEPTH);
    }

    default T setHasMaxDepth(int value) {
        return set(MAX_DEPTH, value);
    }
}
