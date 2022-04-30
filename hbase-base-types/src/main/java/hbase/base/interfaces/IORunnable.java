package hbase.base.interfaces;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Alternative to {@link Runnable} that can throw {@link IOException}
 */
@FunctionalInterface
public interface IORunnable {
    void run() throws IOException;

    default Runnable uncheckedIO(String message) {
        return () -> {
            try {
                run();
            } catch (IOException e) {
                throw new UncheckedIOException(message, e);
            }
        };
    }

    static Runnable uncheckedIO(IORunnable ioRunnable, String message) {
        return ioRunnable.uncheckedIO(message);
    }
}
