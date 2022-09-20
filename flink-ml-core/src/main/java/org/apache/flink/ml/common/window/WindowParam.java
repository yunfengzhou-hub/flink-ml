package org.apache.flink.ml.common.window;

import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidator;

import java.io.IOException;

public class WindowParam extends Param<Window> {
    public WindowParam(
            String name,
            String description,
            Window defaultValue,
            ParamValidator<Window> validator) {
        super(name, Window.class, description, defaultValue, validator);
    }

    @Override
    public Object jsonEncode(Window value) throws IOException {
        return WindowUtils.jsonEncode(value);
    }

    @Override
    public Window jsonDecode(Object json) throws IOException {
        return WindowUtils.jsonDecode(json);
    }
}
