package dev.diogenes.hbase.schema.api.interfaces;

import java.nio.ByteBuffer;
import java.util.Optional;

@FunctionalInterface
public interface BytesSlicer {
    Optional<ByteBuffer> slice(ByteBuffer buffer);

}
