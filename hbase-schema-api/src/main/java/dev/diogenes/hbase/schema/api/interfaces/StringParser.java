package dev.diogenes.hbase.schema.api.interfaces;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface StringParser {
    String parse(ByteBuffer buffer);
}
