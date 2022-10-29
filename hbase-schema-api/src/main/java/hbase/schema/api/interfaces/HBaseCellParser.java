package hbase.schema.api.interfaces;

import java.util.Map;

public interface HBaseCellParser {
    void parse(byte[] qualifier, byte[] value, Map<String, Object> result);
}
