package hbase.schema.api.schema;

import hbase.schema.api.interfaces.BytesParser;
import hbase.schema.api.interfaces.HBaseCellParser;

import java.util.HashMap;
import java.util.Map;

public class HBaseCellPrefixParser<T> implements HBaseCellParser {
    private final String name;
    private final int prefixLength;
    private final BytesParser<String> qualifierParser;
    private final BytesParser<T> valueParser;

    public HBaseCellPrefixParser(String name, byte[] prefix, BytesParser<String> qualifierParser, BytesParser<T> valueParser) {
        this.name = name;
        this.prefixLength = prefix.length;
        this.qualifierParser = qualifierParser;
        this.valueParser = valueParser;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void parse(byte[] qualifier, byte[] value, Map<String, Object> result) {
        if (qualifier.length < prefixLength) {
            return;
        }
        T parsedValue = valueParser.parse(value);
        if (parsedValue == null) {
            return;
        }
        String key = qualifierParser.parse(qualifier, prefixLength, qualifier.length - prefixLength);
        Map<String, T> prefixMap = (Map<String, T>) result.computeIfAbsent(name, n -> new HashMap<>());
        prefixMap.put(key, parsedValue);
    }
}
