package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.diogenes.hbase.schema.api.interfaces.BytesSlicer;
import dev.diogenes.hbase.schema.api.interfaces.NodeParser;
import dev.diogenes.hbase.schema.api.interfaces.ResultParser;
import dev.diogenes.hbase.schema.api.utils.BytesSlicers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class JsonResultParserBuilder {
    private final List<BytesSlicer> rowKeySlicers = new ArrayList<>();
    private final List<String> rowKeyNames = new ArrayList<>();
    private final List<JsonFixedColumnParser> columnParsers = new ArrayList<>();
    private final List<JsonPrefixColumnParser> prefixParsers = new ArrayList<>();

    public JsonResultParserBuilder rowKeySlice(byte separator, String name) {
        rowKeySlicers.add(BytesSlicers.split(separator));
        rowKeyNames.add(name);
        return this;
    }

    public JsonResultParserBuilder rowKeySlice(String name) {
        rowKeySlicers.add(BytesSlicers.remainder());
        rowKeyNames.add(name);
        return this;
    }

    public JsonResultParserBuilder column(String name, NodeParser valueParser) {
        JsonFixedColumnParser parser = new JsonFixedColumnParser(valueParser, name);
        columnParsers.add(parser);
        return this;
    }

    public JsonResultParserBuilder column(String name) {
        return column(name, NodeParsers.utf8NodeParser());
    }

    public JsonResultParserBuilder prefix(String prefix, NodeParser valueParser) {
        JsonPrefixColumnParser parser = new JsonPrefixColumnParser(valueParser, prefix);
        prefixParsers.add(parser);
        return this;
    }

    public JsonResultParserBuilder prefix(String name) {
        return prefix(name, NodeParsers.utf8NodeParser());
    }

    public ResultParser<ObjectNode> build() {
        JsonStringRowKeyParser rowKeyParser = new JsonStringRowKeyParser(rowKeySlicers, rowKeyNames);
        ResultParser<ObjectNode> columnsParser = JsonFixedColumnParser.combineParsers(columnParsers);
        ResultParser<ObjectNode> prefixParser = ResultParser.combineParsers(prefixParsers, JsonNodeFactory.instance::objectNode);

        return new ResultParser<ObjectNode>() {
            @Override
            public boolean parseRowKey(ObjectNode root, ByteBuffer rowKey) {
                return rowKeyParser.parseRowKey(root, rowKey);
            }

            @Override
            public boolean parseCell(ObjectNode root, ByteBuffer column, ByteBuffer value) {
                return columnsParser.parseCell(root, column, value) || prefixParser.parseCell(root, column, value);
            }

            @Override
            public ObjectNode newInstance() {
                return JsonNodeFactory.instance.objectNode();
            }
        };
    }

}
