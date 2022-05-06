package hbase.schema.api.models;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static hbase.schema.api.converters.Utf8Converter.utf8Converter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HBaseGenericRowTest {

    @Test
    void getPrefix_ExtractsPrefix() {
        NavigableMap<byte[], byte[]> cellsMap = HBaseGenericRow.fromPrintableCellsMap(new HashMap<String, String>() {{
            put("a", "A");
            put("b", "B");
            put("c:0", "C");
            put("c:1", "C");
            put("d", "D");
        }});
        byte[] rowKey = utf8("row");
        Map<String, String> expectedPrefixMap = new HashMap<String, String>() {{
            put("0", "C");
            put("1", "C");
        }};

        HBaseGenericRow row = new HBaseGenericRow(rowKey, cellsMap);
        Map<String, String> actualPrefixMap = row.getPrefix(utf8("c:"), utf8Converter());

        assertThat(actualPrefixMap, equalTo(expectedPrefixMap));
    }

    @Test
    void getPrefix_ExtractsPrefix() {
        NavigableMap<byte[], byte[]> cellsMap = HBaseGenericRow.fromPrintableCellsMap(new HashMap<String, String>() {{
            put("a", "A");
            put("b", "B");
            put("c:0", "C");
            put("c:1", "C");
            put("d", "D");
        }});
        byte[] rowKey = utf8("row");
        Map<String, String> expectedPrefixMap = new HashMap<String, String>() {{
            put("0", "C");
            put("1", "C");
        }};

        HBaseGenericRow row = new HBaseGenericRow(rowKey, cellsMap);
        Map<String, String> actualPrefixMap = row.getPrefix(utf8("c:"), utf8Converter());

        assertThat(actualPrefixMap, equalTo(expectedPrefixMap));
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
