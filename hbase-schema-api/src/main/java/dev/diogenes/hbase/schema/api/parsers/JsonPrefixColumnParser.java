package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.diogenes.hbase.schema.api.interfaces.NodeParser;
import dev.diogenes.hbase.schema.api.interfaces.ResultParser;

import java.nio.ByteBuffer;

import static dev.diogenes.hbase.schema.api.parsers.StringParsers.UTF8_PARSER;

public class JsonPrefixColumnParser implements ResultParser<ObjectNode> {
    private final NodeParser parser;
    private final String prefix;

    public JsonPrefixColumnParser(NodeParser parser, String prefix) {
        this.parser = parser;
        this.prefix = prefix;
    }

    @Override
    public boolean parseCell(ObjectNode root, ByteBuffer column, ByteBuffer value) {
        String columnName = UTF8_PARSER.parse(column);
        if (!columnName.startsWith(prefix)) {
            return false;
        }
        JsonNode valueNode = parser.parse(value);
        if (valueNode == null) {
            return false;
        }

        JsonNode prefixNode = root.get(prefix);
        if (!(prefixNode instanceof ObjectNode)) {
            prefixNode = JsonNodeFactory.instance.objectNode();
            root.replace(prefix, prefixNode);
        }

        ObjectNode prefixMapNode = (ObjectNode) prefixNode;
        String key = columnName.substring(prefix.length());

        prefixMapNode.replace(key, valueNode);
        return true;
    }

    @Override
    public ObjectNode newInstance() {
        return JsonNodeFactory.instance.objectNode();
    }
}
