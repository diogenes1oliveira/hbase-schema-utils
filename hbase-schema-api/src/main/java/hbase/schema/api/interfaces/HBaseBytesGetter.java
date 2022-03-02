package hbase.schema.api.interfaces;

public interface HBaseBytesGetter<T> {
    byte[] getBytes(T obj);
}
