package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static dev.diogenes.hbase.schema.api.parsers.NodeParsers.utf8NodeParser;
import static dev.diogenes.hbase.schema.api.testutils.BufferUtils.buffer;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unchecked")
class JsonPrefixColumnParserTest {

    @Test
    void parseCell_ShouldCheckPrefix() {
        JsonPrefixColumnParser parser = new JsonPrefixColumnParser(utf8NodeParser(), "prefix:");

        ObjectNode result = parser.newInstance();
        assertThat(parser.parseCell(result, buffer("other:"), buffer("value")), equalTo(false));
        assertThat(result.isEmpty(), equalTo(true));
    }

    @Test
    void parseCell_ShouldSkipNullValues() {
        JsonPrefixColumnParser parser = new JsonPrefixColumnParser(b -> null, "prefix:");

        ObjectNode result = parser.newInstance();
        assertThat(parser.parseCell(result, buffer("other:"), buffer("value")), equalTo(false));
        assertThat(result.isEmpty(), equalTo(true));
    }

    @Test
    void parseCell_ShouldSetEntries() throws IOException {
        JsonPrefixColumnParser parser = new JsonPrefixColumnParser(utf8NodeParser(), "prefix:");

        ObjectNode resultNode = parser.newInstance();
        assertThat(parser.parseCell(resultNode, buffer("prefix:some"), buffer("value")), equalTo(true));
        assertThat(parser.parseCell(resultNode, buffer("prefix:other"), buffer("stuff")), equalTo(true));

        Map<String, ?> resultMap = new ObjectMapper().treeToValue(resultNode.get("prefix:"), Map.class);
        assertThat(resultMap, equalTo(new HashMap<String, String>() {{
            put("some", "value");
            put("other", "stuff");
        }}));
    }
}
