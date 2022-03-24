package hbase.base.interfaces;

import java.io.IOException;

/**
 * Alternative to {@link java.util.function.Function} that can throw {@link IOException}
 *
 * @param <T> argument type
 * @param <R> result type
 */
@FunctionalInterface
public interface IOFunction<T, R> {
    R apply(T t) throws IOException;
}
