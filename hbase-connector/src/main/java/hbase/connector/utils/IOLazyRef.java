package hbase.connector.utils;

import hbase.base.interfaces.IOSupplier;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class IOLazyRef<T> {
    private final IOSupplier<T> supplier;
    private final AtomicReference<T> ref = new AtomicReference<>();
    private final Object lock = new Object();

    public IOLazyRef(IOSupplier<T> supplier) {
        this.supplier = supplier;
    }

    public T get() throws IOException {
        T value = ref.get();

        if (value == null) {
            synchronized (lock) {
                if (ref.get() == null) {
                    value = supplier.get();
                    ref.set(value);
                }
            }
        }

        return value;
    }
}
