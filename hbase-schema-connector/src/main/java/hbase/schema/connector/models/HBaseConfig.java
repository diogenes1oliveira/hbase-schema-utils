package hbase.schema.connector.models;

import hbase.base.interfaces.Config;
import hbase.base.interfaces.ConfigKey;
import hbase.schema.connector.utils.HBaseConfigUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static java.util.function.UnaryOperator.identity;

public enum HBaseConfig implements ConfigKey {
    COLUMN_FAMILY("hbase.column.family", Bytes::toBytesBinary),
    TABLE_SCHEMAS("hbase.tables", HBaseConfigUtils::toKeyValuePairs);

    private final String key;
    private final Function<String, ?> converter;

    HBaseConfig(String key) {
        this(key, identity());
    }

    HBaseConfig(String key, Function<String, ?> converter) {
        this.key = key;
        this.converter = converter;
    }

    @Override
    public String key() {
        return key;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> @Nullable T fromConfig(Config config) {
        return (T) config.getValue(key, converter);
    }
}
