package hbase.schema.api.interfaces.converters;

@FunctionalInterface
public interface TriConsumer<T, U, V> {
    void accept(T t, U u, V v);
}
