package hbase.schema.api.interfaces;

@FunctionalInterface
public interface HBaseFromBytesSetter<T> {
    void setFromBytes(T obj, byte[] bytes);

    static <T> HBaseFromBytesSetter<T> dummy() {
        return ((obj, bytes) -> {

        });
    }
}
