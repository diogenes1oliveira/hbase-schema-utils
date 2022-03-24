package hbase.base.interfaces;

import java.io.IOException;

/**
 * Alternative to {@link java.util.function.Supplier} that can throw {@link IOException}
 *
 * @param <T> supplied type
 */
@FunctionalInterface
public interface IOSupplier<T> {
    T get() throws IOException;
}
