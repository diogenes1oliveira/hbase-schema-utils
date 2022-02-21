package hbase.test.utils;

import hbase.test.utils.interfaces.HBaseTestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static hbase.test.utils.HBaseTestHelpers.loadPropsFromResource;

/**
 * Manages a global HBase test instance configured via System properties or environment variables
 */
public class HBaseTestInstanceSingleton {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseTestInstanceSingleton.class);

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
            getName(System.getProperties(), System.getenv(), true)
                    .ifPresent(name -> instance = createInstance(name));
            initialized = true;
        }
        return instance;
    }

    /**
     * Gets the instance name from System properties or environment variables
     */
    public static Optional<String> getName() {
        return getName(System.getProperties(), System.getenv(), false);
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
    static Optional<String> getName(Properties systemProps, Map<String, String> env, boolean doLog) {
        String propName = systemProps.getProperty(PROPERTY_NAME, "").trim();
        String envName = env.getOrDefault(ENV_NAME, "").trim();
        String resourceName = "";

        try {
            Properties props = loadPropsFromResource(RESOURCE_NAME);
            resourceName = props.getProperty(PROPERTY_NAME, "");
        } catch (IOException e) {
            // ignore
        }

        if (!envName.isEmpty()) {
            if (doLog) {
                LOGGER.info("Using {}={} from environment", ENV_NAME, envName);
            }
            return Optional.of(envName);
        } else if (!propName.isEmpty()) {
            if (doLog) {
                LOGGER.info("Using {}={} from system properties", PROPERTY_NAME, propName);
            }
            return Optional.of(propName);
        } else if (!resourceName.isEmpty()) {
            if (doLog) {
                LOGGER.info("Using {}={} from classpath:{}", PROPERTY_NAME, resourceName, RESOURCE_NAME);
            }
            return Optional.of(resourceName);
        } else {
            if (doLog) {
                LOGGER.info("No configured test instance");
            }
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
