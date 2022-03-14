package hbase.connector.services;

import hbase.base.interfaces.ConfigEnumType;
import hbase.base.interfaces.NamedType;

import java.time.Duration;
import java.util.Properties;

public enum HBaseConnectorConfig implements ConfigEnumType {
    RECONNECTION_PERIOD("hbase.reconnection.period<Duration>", Duration.class),
    LOCK_READ_TIMEOUT("hbase.lock.read.timeout", Duration.class),
    LOCK_WRITE_TIMEOUT("hbase.lock.write.timeout", Duration.class),
    PREFIX("hbase.conf.", Properties.class);

    private final String name;
    private final Class<?> type;

    HBaseConnectorConfig(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }

}
