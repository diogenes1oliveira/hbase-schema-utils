package hbase.test.utils;

import hbase.test.utils.interfaces.HBaseTestInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;

import static hbase.test.utils.HBaseTestHelpers.loadPropsFromResource;

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

    /**
     * Name of a classpath .properties resource to get the instance name from
     */
    public static final String RESOURCE_NAME = "hbase.test.instance.type.properties";

    private static boolean initialized = false;
    private static HBaseTestInstance instance;
    private static final List<HBaseTestInstance> availableInstances = loadAvailableInstances();

    /**
     * Gets the configured instance
     *
     * @throws IllegalStateException no valid instance configured via System properties or environment variables
     */
    public static HBaseTestInstance instance() {
        if (!initialized) {
            getName()
                    .ifPresent(name -> instance = createInstance(name));
            initialized = true;
        }
        return instance;
    }

    /**
     * Gets the instance name from System properties or environment variables
     */
    public static Optional<String> getName() {
        return getName(System.getProperties(), System.getenv());
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
    static Optional<String> getName(Properties systemProps, Map<String, String> env) {
        String propName = systemProps.getProperty(PROPERTY_NAME, "").trim();
        String envName = env.getOrDefault(ENV_NAME, "").trim();
        String resourceName = "";

        try {
            Properties props = loadPropsFromResource(RESOURCE_NAME);
            resourceName = props.getProperty(PROPERTY_NAME, "");
        } catch (IOException e) {
            // ignore
        }

        if (!propName.isEmpty()) {
            return Optional.of(propName);
        } else if (!envName.isEmpty()) {
            return Optional.of(envName);
        } else if (!resourceName.isEmpty()) {
            return Optional.of(resourceName);
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
