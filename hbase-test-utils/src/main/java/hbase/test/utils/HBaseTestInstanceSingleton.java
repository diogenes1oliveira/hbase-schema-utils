package hbase.test.utils;

import hbase.test.utils.interfaces.HBaseTestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * Manages a global HBase test instance configured via System properties or environment variables
 */
public class HBaseTestInstanceSingleton {
    /**
     * Name of the System property to get the instance name from
     */
    public static final String PROPERTY_NAME = "hbase.test.instance.type";

    /**
     * Name of the environment variable to get the instance name from
     */
    public static final String ENV_NAME = "HBASE_TEST_INSTANCE_TYPE";

    private static boolean initialized = false;
    private static HBaseTestInstance instance;
    private static final List<HBaseTestInstance> availableInstances = loadAvailableInstances();

    /**
     * Gets the configured instance
     *
     * @throws IllegalStateException no valid instance configured via System properties or environment variables
     */
    public static Optional<HBaseTestInstance> instance() {
        if (!initialized) {
            getSingletonName()
                    .ifPresent(name -> instance = createInstance(name));
            initialized = true;
        }
        return Optional.ofNullable(instance);
    }

    /**
     * Gets the instance name from System properties or environment variables
     */
    public static Optional<String> getSingletonName() {
        return getSingletonName(System.getProperties(), System.getenv());
    }

    /**
     * Loads all available instances via {@link ServiceLoader}
     */
    public static List<HBaseTestInstance> loadAvailableInstances() {
        List<HBaseTestInstance> instances = new ArrayList<>();
        ServiceLoader.load(HBaseTestInstance.class).forEach(instances::add);
        return instances;
    }

    // package-private, just for tests
    static Optional<String> getSingletonName(Properties systemProps, Map<String, String> env) {
        String propName = systemProps.getProperty(PROPERTY_NAME, "").trim();
        String envName = env.getOrDefault(ENV_NAME, "").trim();

        if (!propName.isEmpty()) {
            return Optional.of(propName);
        } else if (!envName.isEmpty()) {
            return Optional.of(envName);
        } else {
            return Optional.empty();
        }
    }

    private static HBaseTestInstance createInstance(String name) {
        return availableInstances.stream()
                                 .filter(instance -> name.equals(instance.name()))
                                 .findFirst()
                                 .orElseThrow(() ->
                                         new IllegalStateException("No instance named '" + name + "' is available")
                                 );
    }

}
