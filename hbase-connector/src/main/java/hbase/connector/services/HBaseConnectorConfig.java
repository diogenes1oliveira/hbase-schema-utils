package hbase.connector.services;

import hbase.base.interfaces.Config;
import hbase.base.interfaces.ConfigKey;

public enum HBaseConnectorConfig implements ConfigKey {
    RECONNECTION_PERIOD("hbase.reconnection.period"),
    LOCK_READ_TIMEOUT("hbase.lock.read.timeout"),
    LOCK_WRITE_TIMEOUT("hbase.lock.write.timeout"),
    PREFIX("hbase.conf.");

    private final String name;

    HBaseConnectorConfig(String name) {
        this.name = name;
    }

    @Override
    public String key() {
        return name;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T fromConfig(Config config) {
        T value;
        if (this == PREFIX) {
            value = (T) config.getPrefix(name);
        } else {
            value = (T) config.getValue(name, Long::parseLong);
        }

        if (value == null) {
            throw new IllegalArgumentException("No value for " + name);
        }

        return value;
    }
}
