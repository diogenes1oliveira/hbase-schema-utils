package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.diogenes.hbase.schema.api.utils.BytesSlicers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static dev.diogenes.hbase.schema.api.testutils.BufferUtils.buffer;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class JsonStringRowKeyParserTest {

    @Test
    void parseRowKey_HandlesInvalidParseResults() {
        JsonStringRowKeyParser parser = new JsonStringRowKeyParser(
                asList(
                        BytesSlicers.split('|'),
                        BytesSlicers.remainder()
                ),
                asList("first", "last")
        );
        String input = "single";

        ObjectNode result = JsonNodeFactory.instance.objectNode();
        assertThat(parser.parseRowKey(result, buffer(input)), equalTo(false));

        assertThat(result.isEmpty(), equalTo(true));
    }


    @Test
    void parseRowKey_SplitsBySeparator() {
        JsonStringRowKeyParser parser = new JsonStringRowKeyParser(
                asList(
                        BytesSlicers.split('|'),
                        BytesSlicers.split('|'),
                        BytesSlicers.remainder()
                ),
                asList("first", "second", "last")
        );
        String input = "left|middle|right";

        ObjectNode result = JsonNodeFactory.instance.objectNode();
        assertThat(parser.parseRowKey(result, buffer(input)), equalTo(true));

        assertThat(result.get("first").asText(), equalTo("left"));
        assertThat(result.get("second").asText(), equalTo("middle"));
        assertThat(result.get("last").asText(), equalTo("right"));
    }

    @Test
    void parseRowKey_CanMixSeparatorAndFixedSlice() {
        JsonStringRowKeyParser parser = new JsonStringRowKeyParser(
                asList(
                        BytesSlicers.fixed(4),
                        BytesSlicers.split('|'),
                        BytesSlicers.remainder()
                ),
                asList("first", "second", "last")
        );
        String input = "leftmiddle|right";

        ObjectNode result = JsonNodeFactory.instance.objectNode();
        assertThat(parser.parseRowKey(result, buffer(input)), equalTo(true));

        assertThat(result.get("first").asText(), equalTo("left"));
        assertThat(result.get("second").asText(), equalTo("middle"));
        assertThat(result.get("last").asText(), equalTo("right"));
    }
}
