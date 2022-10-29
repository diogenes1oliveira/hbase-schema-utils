package hbase.schema.api.schema;

import hbase.schema.api.interfaces.BytesParser;
import hbase.schema.api.interfaces.HBaseCellParser;
import hbase.schema.api.interfaces.HBaseParser;
import hbase.schema.api.interfaces.HBaseRowParser;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static hbase.schema.api.converters.Utf8BytesParser.UTF8_BYTES_PARSER;
import static hbase.schema.api.utils.BytesPrefixComparator.BYTES_PREFIX_COMPARATOR;

public class HBaseParserBuilder {
    private final byte[] columnFamily;
    private HBaseRowParser rowParser = null;
    private int rowPartsCount = 0;
    private final SortedMap<byte[], HBaseCellParser> parsersByQualifier = new TreeMap<>(BYTES_PREFIX_COMPARATOR);

    public HBaseParserBuilder(String columnFamily) {
        this.columnFamily = columnFamily.getBytes(StandardCharsets.UTF_8);
    }

    public HBaseParserBuilder separator(String separator) {
        rowParser = new HBaseStringRowParser(separator);
        return this;
    }

    private HBaseStringRowParser assureStringRowParser() {
        if (rowParser == null) {
            throw new IllegalStateException("No row parser has been set yet");
        } else if (!(rowParser instanceof HBaseStringRowParser)) {
            throw new IllegalStateException("Incorrect type for current row parser");
        } else {
            return (HBaseStringRowParser) rowParser;
        }
    }

    public HBaseParserBuilder constant(String value) {
        assureStringRowParser();
        ++rowPartsCount;
        return this;
    }

    public <T> HBaseParserBuilder fragment(String name, BytesParser<T> partParser) {
        rowParser = assureStringRowParser().withPart(name, rowPartsCount++, partParser);
        return this;
    }

    public HBaseParserBuilder fragment(String name) {
        return fragment(name, UTF8_BYTES_PARSER);
    }

    public <T> HBaseParserBuilder column(String qualifier, BytesParser<T> valueParser) {
        parsersByQualifier.put(qualifier.getBytes(StandardCharsets.UTF_8), new HBaseCellColumnParser<>(qualifier, valueParser));
        return this;
    }

    public <T> HBaseParserBuilder column(String qualifier) {
        return column(qualifier, UTF8_BYTES_PARSER);
    }

    public <T> HBaseParserBuilder prefix(String prefix, BytesParser<T> valueParser, BytesParser<String> qualifierParser) {
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        parsersByQualifier.put(prefixBytes, new HBaseCellPrefixParser<>(prefix, prefixBytes, qualifierParser, valueParser));
        return this;
    }

    public <T> HBaseParserBuilder prefix(String prefix, BytesParser<T> valueParser) {
        return prefix(prefix, valueParser, UTF8_BYTES_PARSER);
    }

    public HBaseParserBuilder prefix(String prefix) {
        return prefix(prefix, UTF8_BYTES_PARSER, UTF8_BYTES_PARSER);
    }

    public HBaseParser build() {
        return hBaseResult -> {
            Map<String, Object> parseResult = new LinkedHashMap<>();

            byte[] rowKey = hBaseResult.getRow();
            if (rowKey != null && rowParser != null) {
                rowParser.parse(rowKey, parseResult);
            }

            for (Map.Entry<byte[], byte[]> cellEntry : hBaseResult.getFamilyMap(columnFamily).entrySet()) {
                byte[] qualifier = cellEntry.getKey();
                byte[] value = cellEntry.getValue();
                if (value == null || qualifier == null) {
                    // just in case...
                    continue;
                }
                HBaseCellParser parser = parsersByQualifier.get(qualifier);
                if (parser == null) {
                    continue;
                }
                parser.parse(qualifier, value, parseResult);
            }
            return parseResult;
        };
    }
}
