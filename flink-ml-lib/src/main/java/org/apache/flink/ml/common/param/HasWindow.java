package org.apache.flink.ml.common.param;

import org.apache.flink.ml.common.window.BoundedWindow;
import org.apache.flink.ml.common.window.Window;
import org.apache.flink.ml.common.window.WindowParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.WithParams;

@SuppressWarnings("rawtypes")
public interface HasWindow<T> extends WithParams<T> {
    Param<Window> WINDOW =
            new WindowParam(
                    "window", "window parameter.", BoundedWindow.get(), ParamValidators.notNull());

    default T setWindow(Window window) {
        return set(WINDOW, window);
    }

    default Window getWindow() {
        return get(WINDOW);
    }
}
