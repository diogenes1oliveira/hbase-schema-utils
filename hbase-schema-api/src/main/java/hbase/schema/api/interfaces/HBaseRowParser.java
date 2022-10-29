package hbase.schema.api.interfaces;

import java.util.Map;

public interface HBaseRowParser {
    void parse(byte[] rowKey, Map<String, Object> result);
}
