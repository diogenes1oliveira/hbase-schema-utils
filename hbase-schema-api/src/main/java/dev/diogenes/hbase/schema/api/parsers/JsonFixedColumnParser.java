package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.diogenes.hbase.schema.api.interfaces.NodeParser;
import dev.diogenes.hbase.schema.api.interfaces.ResultParser;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static dev.diogenes.hbase.schema.api.parsers.StringParsers.UTF8_PARSER;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toMap;

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
        return parseCellUnchecked(root, value);
    }

    private boolean parseCellUnchecked(ObjectNode root, ByteBuffer value) {
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

    public static ResultParser<ObjectNode> combineAll(List<JsonFixedColumnParser> parsers) {
        Map<String, JsonFixedColumnParser> parsersByName = parsers.stream().collect(toMap(p -> p.name, identity()));
        return new ResultParser<ObjectNode>() {
            @Override
            public boolean parseCell(ObjectNode root, ByteBuffer column, ByteBuffer value) {
                String columnName = UTF8_PARSER.parse(column);
                JsonFixedColumnParser parser = parsersByName.get(columnName);
                if (parser == null) {
                    return false;
                }
                return parser.parseCellUnchecked(root, value);
            }

            @Override
            public ObjectNode newInstance() {
                return JsonNodeFactory.instance.objectNode();
            }
        };
    }
}
