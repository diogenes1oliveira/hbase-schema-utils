package dev.diogenes.hbase.schema.api.interfaces;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface NodeParser {
    JsonNode parse(ByteBuffer buffer);
}
