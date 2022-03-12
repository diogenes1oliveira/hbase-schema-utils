package hbase.base.testutils;

import hbase.base.interfaces.Config;
import hbase.base.interfaces.Configurable;

public class DummyConfigurable implements Configurable {
    private String string;
    private int intValue;
    private double doubleValue;

    public String getString() {
        return string;
    }

    public int getInt() {
        return intValue;
    }

    public double getDouble() {
        return doubleValue;
    }

    @Override
    public void configure(Config config) {
        string = config.getValue("some.string");
        intValue = config.getValue("some.int", -1, Integer.class);
        doubleValue = config.getValue("some.double", 0.0, Double.class);
    }

}
