package hbase.connector.interfaces;

import java.io.IOException;

/**
 * Alternative to {@link java.util.function.Consumer} that can throw {@link IOException}
 *
 * @param <T> consumed type
 */
@FunctionalInterface
public interface IOConsumer<T> {
    void accept(T value) throws IOException;
}
