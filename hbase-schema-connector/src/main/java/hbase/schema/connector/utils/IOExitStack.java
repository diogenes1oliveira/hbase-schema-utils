package hbase.schema.connector.utils;

import hbase.base.interfaces.IORunnable;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

public class IOExitStack implements AutoCloseable {
    private final List<Pair<String, IORunnable>> callbacks = new ArrayList<>();

    public void add(String message, IORunnable onClose) {
        callbacks.add(Pair.of(message, onClose));
    }

    @Override
    public void close() {
        List<RuntimeException> exceptions = new ArrayList<>();

        for (int i = callbacks.size() - 1; i >= 0; --i) {
            String message = callbacks.get(i).getLeft();
            IORunnable callback = callbacks.get(i).getRight();
            try {
                callback.run();
            } catch (IOException e) {
                exceptions.add(new UncheckedIOException(message, e));
            } catch (RuntimeException e) {
                exceptions.add(e);
            }
        }

        if (exceptions.isEmpty()) {
            return;
        }

        RuntimeException main = exceptions.get(0);
        for (int i = 1; i < exceptions.size(); ++i) {
            main.addSuppressed(exceptions.get(i));
        }

        throw main;
    }
}
