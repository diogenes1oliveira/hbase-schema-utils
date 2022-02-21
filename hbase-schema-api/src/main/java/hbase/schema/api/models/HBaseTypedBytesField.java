package hbase.schema.api.models;

import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import hbase.schema.api.interfaces.converters.HBaseBytesParser;

import java.nio.charset.StandardCharsets;

public class HBaseTypedBytesField<T> {
    private final String name;
    private final HBaseBytesParser<T> parser;
    private final HBaseBytesMapper<T> mapper;

    public HBaseTypedBytesField(String name, HBaseBytesParser<T> parser, HBaseBytesMapper<T> mapper) {
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

    public HBaseBytesParser<T> parser() {
        return parser;
    }

    public HBaseBytesMapper<T> mapper() {
        return mapper;
    }

    @SuppressWarnings("UnusedReturnValue")
    public static class Builder<T> {
        private String name;
        private HBaseBytesParser<T> parser = HBaseBytesParser.writeOnly();
        private HBaseBytesMapper<T> mapper = HBaseBytesMapper.readOnly();

        public Builder() {
            this(null);
        }

        public Builder(String name) {
            withName(name);
        }

        public Builder<T> withName(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> withParser(HBaseBytesParser<T> parser) {
            this.parser = parser;
            return this;
        }

        public Builder<T> withMapper(HBaseBytesMapper<T> mapper) {
            this.mapper = mapper;
            return this;
        }

        public HBaseTypedBytesField<T> build() {
            if (name == null) {
                throw new IllegalArgumentException("No name for field");
            }
            return new HBaseTypedBytesField<>(name, parser, mapper);
        }
    }
}
