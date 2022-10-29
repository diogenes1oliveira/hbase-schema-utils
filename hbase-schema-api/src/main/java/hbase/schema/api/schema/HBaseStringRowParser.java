package hbase.schema.api.schema;

import hbase.schema.api.interfaces.BytesParser;
import hbase.schema.api.interfaces.HBaseRowParser;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class HBaseStringRowParser implements HBaseRowParser {
    private final String separator;
    private final List<BytesParser<?>> parsers = new ArrayList<>();
    private final Map<Integer, String> namesMap = new HashMap<>();
    private final Map<String, BytesParser<?>> parsersMap = new HashMap<>();

    public HBaseStringRowParser(String separator) {
        this.separator = Pattern.quote(separator);
    }

    public <T> HBaseStringRowParser withPart(String name, int index, BytesParser<T> parser) {
        namesMap.put(index, name);
        parsersMap.put(name, parser);
        return this;
    }

    @Override
    public void parse(byte[] rowKeyBytes, Map<String, Object> result) {
        String rowKey = new String(rowKeyBytes, StandardCharsets.UTF_8);
        String[] rowKeyParts = rowKey.split(separator);
        for (int i = 0; i < rowKeyParts.length; ++i) {
            String name = namesMap.get(i);
            if (name == null) {
                continue;
            }
            BytesParser<?> parser = parsersMap.get(name);
            if (parser == null) {
                throw new IllegalStateException("No parser for row key part: " + name);
            }
            String rowKeyPart = rowKeyParts[i];
            Object value = parser.parse(rowKeyPart.getBytes(StandardCharsets.UTF_8));
            if (value != null) {
                result.put(name, value);
            }

        }
    }

}
