package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.diogenes.hbase.schema.api.interfaces.NodeParser;
import dev.diogenes.hbase.schema.api.interfaces.ResultParser;

import java.nio.ByteBuffer;

import static dev.diogenes.hbase.schema.api.parsers.StringParsers.UTF8_PARSER;

public class JsonFixedColumnParser implements ResultParser<ObjectNode> {
    private final NodeParser parser;
    private final String name;
    private final boolean checkName;

    public JsonFixedColumnParser(NodeParser parser, String name, boolean checkName) {
        this.parser = parser;
        this.name = name;
        this.checkName = checkName;
    }

    public JsonFixedColumnParser(NodeParser parser, String name) {
        this(parser, name, true);
    }

    @Override
    public boolean parseCell(ObjectNode root, ByteBuffer column, ByteBuffer value) {
        if (checkName) {
            String columnName = UTF8_PARSER.parse(column);
            if (!name.equals(columnName)) {
                return false;
            }
        }
        JsonNode node = parser.parse(value);
        if (node == null) {
            return false;
        }
        root.set(name, node);
        return true;
    }

    @Override
    public ObjectNode newInstance() {
        return JsonNodeFactory.instance.objectNode();
    }
}
