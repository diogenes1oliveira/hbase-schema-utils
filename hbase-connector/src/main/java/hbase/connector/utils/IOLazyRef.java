package hbase.connector.utils;

import hbase.base.interfaces.IOSupplier;
import org.apache.hadoop.hbase.shaded.org.apache.commons.io.function.IOConsumer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implements a lazily initialized reference, where the initialization might throw {@link IOException}
 *
 * @param <T> value type
 */
public class IOLazyRef<T> {
    private final IOSupplier<T> constructor;
    private final AtomicReference<T> ref = new AtomicReference<>();
    private final Object lock = new Object();

    /**
     * @param constructor lambda to create a new object instance
     */
    public IOLazyRef(IOSupplier<T> constructor) {
        this.constructor = constructor;
    }

    /**
     * Gets the current value or calls the constructor to initialize it
     *
     * @throws UncheckedIOException constructor threw an {@link IOException} exception
     * @throws NullPointerException constructor returned a null value
     */
    public T get() {
        T value = ref.get();

        if (value == null) {
            synchronized (lock) {
                if (ref.get() == null) {
                    try {
                        value = constructor.get();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    if (value == null) {
                        throw new NullPointerException("Object supplier returned null");
                    }
                    ref.set(value);
                }
            }
        }

        return value;
    }

    /**
     * Clears and returns the current value
     *
     * @return value before removal
     */
    public T remove() {
        return ref.getAndSet(null);
    }

    /**
     * Removes the current value and calls the closer function if it's not null
     *
     * @param closer lambda to be called if the value is set
     * @throws UncheckedIOException closer threw an {@link IOException} exception
     */
    public void remove(IOConsumer<T> closer) {
        T value = remove();
        if (value != null) {
            try {
                closer.accept(value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
