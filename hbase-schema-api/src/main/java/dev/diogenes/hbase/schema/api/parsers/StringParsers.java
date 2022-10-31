package dev.diogenes.hbase.schema.api.parsers;

import dev.diogenes.hbase.schema.api.interfaces.StringParser;

import java.nio.charset.StandardCharsets;

public final class StringParsers {
    public static final StringParser UTF8_PARSER = buffer -> StandardCharsets.UTF_8.decode(buffer).toString();
}
