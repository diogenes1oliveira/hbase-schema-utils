package dev.diogenes.hbase.schema.api.parsers;

import dev.diogenes.hbase.schema.api.interfaces.BytesSlicer;
import dev.diogenes.hbase.schema.api.interfaces.ResultParser;
import dev.diogenes.hbase.schema.api.interfaces.StringParser;
import dev.diogenes.hbase.schema.api.utils.BytesSlicers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class StringRowKeyParser implements ResultParser<List<String>> {
    private final List<BytesSlicer> slicers;
    private final StringParser parser;

    public StringRowKeyParser(StringParser parser, List<BytesSlicer> slicers) {
        this.slicers = slicers;
        this.parser = parser;
    }

    public StringRowKeyParser(List<BytesSlicer> slicers) {
        this(StringParsers.UTF8_PARSER, slicers);
    }

    public StringRowKeyParser(BytesSlicer... slicers) {
        this(asList(slicers));
    }

    @Override
    public boolean parseRowKey(List<String> result, ByteBuffer rowKey) {
        List<ByteBuffer> slices = BytesSlicers.toSlices(rowKey, slicers);
        if (slices.size() != slicers.size()) {
            return false;
        }

        result.clear();

        for (ByteBuffer slice : slices) {
            String s = parser.parse(slice);
            result.add(s);
        }

        return true;
    }

    @Override
    public List<String> newInstance() {
        return new ArrayList<>();
    }
}
