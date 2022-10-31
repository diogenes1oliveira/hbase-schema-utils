package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.diogenes.hbase.schema.api.interfaces.NodeParser;
import dev.diogenes.hbase.schema.api.interfaces.ResultParser;

import java.util.HashMap;
import java.util.Map;

public class JsonResultParserBuilder {
    private ObjectMapper mapper = new ObjectMapper();

    private final Map<String, NodeParser> columnParsers = new HashMap<>();
//    private final List

    public JsonResultParserBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ResultParser<JsonNode> build() {
        // TODO: complete this builder
        return null;
    }

}
