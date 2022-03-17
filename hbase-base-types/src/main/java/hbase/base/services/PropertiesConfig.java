package hbase.base.services;

import hbase.base.interfaces.Config;
import org.jetbrains.annotations.Nullable;

import java.util.Properties;

/**
 * Config object that uses a {@link Properties} object as the value source
 */
public class PropertiesConfig implements Config {
    private final Properties props;

    /**
     * @param props source {@link Properties} object
     */
    public PropertiesConfig(Properties props) {
        this.props = props;
    }

    /**
     * Uses {@link System#getProperties()} as the property source
     */
    public PropertiesConfig() {
        this(System.getProperties());
    }

    /**
     * Selects the properties that start with the given key prefix
     *
     * @param prefix config prefix
     */
    @Override
    public Properties getPrefix(String prefix) {
        Properties result = new Properties();

        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(prefix)) {
                String value = props.getProperty(name);
                result.setProperty(name.substring(prefix.length()), value);
            }
        }

        return result;
    }

    /**
     * Gets a property value
     */
    @Override
    public @Nullable String getValue(String name) {
        return props.getProperty(name);
    }

    @Override
    public <T> @Nullable T getValue(String name, Class<?> type) {
        throw new IllegalArgumentException("Unsupported conversion for " + type);
    }
}
