package hbase.base.helpers;

import hbase.base.interfaces.Config;
import hbase.base.interfaces.Configurable;

import java.util.Properties;
import java.util.function.Predicate;

/**
 * Helper methods to deal with configurations
 */
public final class ConfigUtils {
    private ConfigUtils() {
        // utility class
    }


    private static void configurePrefix(Configurable configurable, Config config, String prefix) {
        Properties configProps = config.getPrefix(prefix);
        Properties props = new Properties();

        for (String name : configProps.stringPropertyNames()) {
            String unprefixedName = name.substring(prefix.length());
            String value = configProps.getProperty(name);
            props.setProperty(unprefixedName, value);
        }

        configurable.configure(prefix, props);
    }

    private static void configureMandatory(Configurable configurable, Config config, String name) {
        String value = config.getValue(name);
        if (value == null) {
            throw new IllegalArgumentException("No value for configuration " + name);
        }
        configurable.configure(name, config);
    }

    private static void configureNullable(Configurable configurable, Config config, String name) {
        configurable.configureNullable(name, config);
    }

    /**
     * Effects the configuration
     *
     * @param configurable configurable instance
     * @param config       config object
     * @param configSpec   config spec, such as documented in {@link Configurable#configs()}
     * @throws IllegalArgumentException bad config value type
     */
    public static void configure(Configurable configurable, Config config, String configSpec) {
        String name = configSpec;

        try {
            if (configSpec.endsWith("?")) {
                name = configSpec.substring(0, configSpec.length() - 1);
                if (config.hasConfig(name)) {
                    configureNullable(configurable, config, name);
                }
            } else if (configSpec.endsWith(".")) {
                configurePrefix(configurable, config, configSpec);
            } else if (config.hasConfig(configSpec)) {
                configureMandatory(configurable, config, configSpec);
            }
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Invalid type for configuration " + name);
        }
    }

    /**
     * Selects the properties that match a given test
     *
     * @param source      source {@link Properties} object
     * @param keySelector predicate to select the keys to return
     * @return new filtered {@link Properties} object
     */
    public static Properties filterProperties(Properties source, Predicate<String> keySelector) {
        Properties props = new Properties();

        for (String key : source.stringPropertyNames()) {
            if (keySelector.test(key)) {
                String value = source.getProperty(key);
                props.setProperty(key, value);
            }
        }

        return props;
    }
}
