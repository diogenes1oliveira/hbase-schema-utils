package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static dev.diogenes.hbase.schema.api.parsers.NodeParsers.utf8NodeParser;
import static dev.diogenes.hbase.schema.api.testutils.BufferUtils.buffer;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("unchecked")
class JsonFixedColumnParserTest {

    @Test
    void parseCell_ShouldSetCellValue() throws IOException {
        JsonFixedColumnParser parser = new JsonFixedColumnParser(utf8NodeParser(), "qualifier");

        ObjectNode resultNode = parser.newInstance();
        assertThat(parser.parseCell(resultNode, buffer("qualifier"), buffer("value")), equalTo(true));

        Map<String, ?> resultMap = new ObjectMapper().treeToValue(resultNode, Map.class);
        assertThat(resultMap, equalTo(singletonMap("qualifier", "value")));
    }

    @Test
    void parseCell_ShouldCheckName() {
        JsonFixedColumnParser parser = new JsonFixedColumnParser(utf8NodeParser(), "qualifier", true);

        ObjectNode result = parser.newInstance();
        assertThat(parser.parseCell(result, buffer("other"), buffer("value")), equalTo(false));
        assertThat(result.isEmpty(), equalTo(true));
    }

    @Test
    void parseCell_ShouldSkipCheckName() throws IOException {
        JsonFixedColumnParser parser = new JsonFixedColumnParser(utf8NodeParser(), "qualifier", false);

        ObjectNode resultNode = parser.newInstance();
        assertThat(parser.parseCell(resultNode, buffer("other"), buffer("value")), equalTo(true));

        Map<String, ?> resultMap = new ObjectMapper().treeToValue(resultNode, Map.class);
        assertThat(resultMap, equalTo(singletonMap("qualifier", "value")));
    }

    @Test
    void parseCell_ShouldSkipNullParseResults() {
        JsonFixedColumnParser parser = new JsonFixedColumnParser(b -> null, "qualifier");

        ObjectNode result = parser.newInstance();
        assertThat(parser.parseCell(result, buffer("qualifier"), buffer("value")), equalTo(false));
        assertThat(result.isEmpty(), equalTo(true));
    }

}
