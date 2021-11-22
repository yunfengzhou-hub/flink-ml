package org.apache.flink.ml.tree.impurity;

public class Impurities {
    static String[] supportedNames = new String[]{
            "entropy",
    };

    public static String[] getSupportedNames() {
        return supportedNames;
    }

    public static Impurity fromString(String name) {
        switch (name) {
            case "entropy":
                return new Entropy();
            default:
                throw new IllegalArgumentException(String.format("Did not recognize Impurity name: {}", name));
        }
    }
}
