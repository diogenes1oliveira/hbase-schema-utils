package hbase.connector.utils;

import hbase.base.interfaces.IORunnable;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class IOExitStack implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(IOExitStack.class);

    private final ConcurrentLinkedDeque<Pair<String, IORunnable>> callbacks = new ConcurrentLinkedDeque<>();
    private final String name;

    public IOExitStack(String name) {
        this.name = name;

        LOGGER.info("Entering {}: {}", IOExitStack.class.getSimpleName(), name);
    }

    public void add(String message, IORunnable onClose) {
        callbacks.push(Pair.of(message, onClose));
    }

    @Override
    public void close() {

        LOGGER.info("Starting to close {}: {}", IOExitStack.class.getSimpleName(), name);
        List<RuntimeException> exceptions = new ArrayList<>();

        while (!callbacks.isEmpty()) {
            Pair<String, IORunnable> pair = callbacks.pop();
            String message = pair.getLeft();
            IORunnable callback = pair.getRight();
            try {
                callback.run();
            } catch (IOException e) {
                exceptions.add(new UncheckedIOException(message, e));
            } catch (RuntimeException e) {
                exceptions.add(e);
            }
        }

        LOGGER.info("Ran all finalizers of {}: {}", IOExitStack.class.getSimpleName(), name);
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
