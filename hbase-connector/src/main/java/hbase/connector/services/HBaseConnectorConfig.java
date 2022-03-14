package hbase.connector.services;

import hbase.base.interfaces.Config;
import hbase.base.interfaces.ConfigKey;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

public enum HBaseConnectorConfig implements ConfigKey {
    RECONNECTION_PERIOD("hbase.reconnection.period", s -> Duration.parse(s).toMillis()),
    LOCK_READ_TIMEOUT("hbase.lock.read.timeout", s -> Duration.parse(s).toMillis()),
    LOCK_WRITE_TIMEOUT("hbase.lock.write.timeout", s -> Duration.parse(s).toMillis()),
    PREFIX("hbase.conf.", Config::getPrefix);

    private final String key;
    private final Function<Config, ?> getter;

    HBaseConnectorConfig(String key, Function<String, ?> converter) {
        this(key, (config, n) -> config.getValue(n, converter));
    }

    HBaseConnectorConfig(String key, BiFunction<Config, String, ?> getter) {
        this.key = key;
        this.getter = config -> getter.apply(config, key);
    }

    @Override
    public String key() {
        return key;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T fromConfig(Config config) {
        return (T) getter.apply(config);
    }
}
