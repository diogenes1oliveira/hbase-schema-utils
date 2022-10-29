package hbase.schema.api.schema;

import hbase.schema.api.interfaces.BytesParser;
import hbase.schema.api.interfaces.HBaseCellParser;

import java.util.Map;

public class HBaseCellColumnParser<T> implements HBaseCellParser {
    private final String name;
    private final BytesParser<T> valueParser;

    public HBaseCellColumnParser(String name, BytesParser<T> valueParser) {
        this.name = name;
        this.valueParser = valueParser;
    }

    @Override
    public void parse(byte[] qualifier, byte[] value, Map<String, Object> result) {
        T parsed = valueParser.parse(value);
        if (parsed != null) {
            result.put(name, parsed);
        }
    }
}
