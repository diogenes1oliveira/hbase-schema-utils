package hbase.base.interfaces;

/**
 * Similar to {@link java.util.function.BiConsumer}, but accepts three arguments
 */
@FunctionalInterface
public interface TriConsumer<T, U, V> {
    void accept(T t, U u, V v);
}
