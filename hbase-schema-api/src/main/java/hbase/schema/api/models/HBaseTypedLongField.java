package hbase.schema.api.models;

import hbase.schema.api.interfaces.converters.HBaseLongMapper;
import hbase.schema.api.interfaces.converters.HBaseLongParser;

import java.nio.charset.StandardCharsets;

public class HBaseTypedLongField<T> {
    private final String name;
    private final HBaseLongParser<T> parser;
    private final HBaseLongMapper<T> mapper;

    public HBaseTypedLongField(String name, HBaseLongParser<T> parser, HBaseLongMapper<T> mapper) {
        this.name = name;
        this.parser = parser;
        this.mapper = mapper;
    }

    public String name() {
        return name;
    }

    public byte[] nameBytes() {
        return name.getBytes(StandardCharsets.UTF_8);
    }

    public HBaseLongParser<T> parser() {
        return parser;
    }

    public HBaseLongMapper<T> mapper() {
        return mapper;
    }


    @SuppressWarnings("UnusedReturnValue")
    public static class Builder<T> {
        private String name;
        private HBaseLongParser<T> parser = HBaseLongParser.writeOnly();
        private HBaseLongMapper<T> mapper = HBaseLongMapper.readOnly();

        public Builder() {
            this(null);
        }

        public Builder(String name) {
            withName(name);
        }

        public HBaseTypedLongField.Builder<T> withName(String name) {
            this.name = name;
            return this;
        }

        public HBaseTypedLongField.Builder<T> withParser(HBaseLongParser<T> parser) {
            this.parser = parser;
            return this;
        }

        public HBaseTypedLongField.Builder<T> withMapper(HBaseLongMapper<T> mapper) {
            this.mapper = mapper;
            return this;
        }

        public HBaseTypedLongField<T> build() {
            return new HBaseTypedLongField<>(name, parser, mapper);
        }
    }
}
