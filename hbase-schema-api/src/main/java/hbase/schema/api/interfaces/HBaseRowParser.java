package hbase.schema.api.interfaces;

public interface HBaseRowParser<T> {
    void parseRowKey(T object, byte[] rowKey);
}
