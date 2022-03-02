package hbase.schema.api.interfaces.converters;

@FunctionalInterface
public interface HBaseBytesSetter<T> {
    void setFromBytes(T obj, byte[] bytes);

    static <T> HBaseBytesSetter<T> dummy() {
        return ((obj, bytes) -> {

        });
    }
}
