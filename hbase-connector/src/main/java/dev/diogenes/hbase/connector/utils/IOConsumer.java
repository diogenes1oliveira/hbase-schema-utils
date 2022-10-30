package dev.diogenes.hbase.connector.utils;

import java.io.IOException;

@SuppressWarnings("unchecked")
public interface IOConsumer<T> {
    void accept(T t) throws IOException;

    IOConsumer<?> DUMMY = t -> {
    };

    static <T> IOConsumer<T> dummy() {
        return (IOConsumer<T>) DUMMY;
    }

}
