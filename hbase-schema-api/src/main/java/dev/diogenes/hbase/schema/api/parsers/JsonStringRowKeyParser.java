package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.diogenes.hbase.schema.api.interfaces.BytesSlicer;
import dev.diogenes.hbase.schema.api.interfaces.ResultParser;
import dev.diogenes.hbase.schema.api.interfaces.StringParser;
import dev.diogenes.hbase.schema.api.utils.BytesSlicers;

import java.nio.ByteBuffer;
import java.util.List;

public class JsonStringRowKeyParser implements ResultParser<ObjectNode> {
    private final List<BytesSlicer> slicers;
    private final List<String> names;
    private final StringParser parser;

    public JsonStringRowKeyParser(List<BytesSlicer> slicers, List<String> names) {
        if (slicers.size() != names.size()) {
            throw new IllegalArgumentException("non-matching identifiers");
        }
        this.slicers = slicers;
        this.parser = StringParsers.UTF8_PARSER;
        this.names = names;
    }

    @Override
    public boolean parseRowKey(ObjectNode result, ByteBuffer rowKey) {
        List<ByteBuffer> slices = BytesSlicers.toSlices(rowKey, slicers);
        if (slices.size() != slicers.size()) {
            return false;
        }

        for (int i = 0; i < slices.size(); ++i) {
            ByteBuffer slice = slices.get(i);
            String value = parser.parse(slice);
            String name = names.get(i);

            result.put(name, value);
        }

        return true;
    }

    @Override
    public ObjectNode newInstance() {
        return JsonNodeFactory.instance.objectNode();
    }

}
