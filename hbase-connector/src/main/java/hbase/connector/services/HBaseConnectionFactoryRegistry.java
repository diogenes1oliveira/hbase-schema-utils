package hbase.connector.services;

import hbase.connector.interfaces.HBaseConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Comparator.comparingInt;

/**
 * Loads the available factories via {@link ServiceLoader}
 * <p>
 * Obs: DO NOT register this class in META-INF/services
 */
public final class HBaseConnectionFactoryRegistry implements HBaseConnectionFactory {
    private final List<HBaseConnectionFactory> factories = new CopyOnWriteArrayList<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * Creates a new connection
     * <p>
     * This will load the available factories via {@link ServiceLoader} the first time it's called
     *
     * @param conf Hadoop-style configuration for the new connection
     * @return new connection
     * @throws IOException no factory for configuration or failed to create with the found one
     */
    @Override
    public synchronized Connection create(Configuration conf) throws IOException {
        if (initialized.compareAndSet(false, true)) {
            loadFactories();
        }
        HBaseConnectionFactory factory = getFactory(conf).orElseThrow(
                () -> new IOException("No factory that supports the given configuration")
        );
        return factory.create(conf);
    }

    /**
     * @param conf Hadoop-style configuration for the new connection
     * @return true if any factory can handle the configuration
     */
    @Override
    public boolean supports(Configuration conf) {
        return getFactory(conf).isPresent();
    }

    /**
     * Gets the factory with the highest priority that supports the configuration
     *
     * @param conf Hadoop-style configuration
     * @return factory for the given configuration or empty
     */
    public Optional<HBaseConnectionFactory> getFactory(Configuration conf) {
        return factories.stream()
                        .filter(factory -> factory.supports(conf))
                        .max(comparingInt(f -> f.priority(conf)));
    }

    private void loadFactories() {
        for (HBaseConnectionFactory factory : ServiceLoader.load(HBaseConnectionFactory.class)) {
            if (factory == this) {
                String thisClassName = HBaseConnectionFactoryRegistry.class.getSimpleName();
                throw new IllegalStateException("The class " + thisClassName + " shouldn't be registered as a service");
            }
            factories.add(factory);
        }
    }
}
