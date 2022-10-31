package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static dev.diogenes.hbase.schema.api.testutils.BufferUtils.buffer;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class NodeParsersTest {

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {"á"})
    void utf8NodeParser(String example) {
        ByteBuffer input = buffer(example);
        JsonNode result = NodeParsers.utf8NodeParser().parse(input);

        assertThat(result.asText(), equalTo(example));
    }

    @Test
    void hexNodeParser() {
        ByteBuffer input = ByteBuffer.wrap(new byte[]{'A', 'B'});
        JsonNode result = NodeParsers.hexNodeParser().parse(input);

        assertThat(result.asText(), equalTo("4142"));
    }

    @Test
    void base64NodeParser() {
        ByteBuffer input = ByteBuffer.wrap(new byte[]{'h', 'i'});
        JsonNode result = NodeParsers.base64NodeParser().parse(input);

        assertThat(result.asText(), equalTo("aGk="));
    }

    @Test
    void stringBinaryNodeParser() {
        ByteBuffer input = ByteBuffer.wrap(new byte[]{'A', 3});
        JsonNode result = NodeParsers.stringBinaryNodeParser().parse(input);

        assertThat(result.asText(), equalTo("A\\x03"));
    }

    @ParameterizedTest
    @ValueSource(longs = {42, -333333})
    void longNodeParser(long example) {
        ByteBuffer input = ByteBuffer.wrap(Bytes.toBytes(example));
        JsonNode result = NodeParsers.longNodeParser().parse(input);

        assertThat(result.asLong(), equalTo(example));
    }

    @SuppressWarnings("unchecked")
    @Test
    void jsonNodeParser() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ByteBuffer input = buffer("{\"answer\": 42}");

        ObjectNode resultNode = (ObjectNode) NodeParsers.jsonNodeParser(mapper).parse(input);
        Map<String, ?> result = mapper.treeToValue(resultNode, Map.class);

        assertThat(result, equalTo(singletonMap("answer", 42)));
    }
}
