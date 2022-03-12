package hbase.base.helpers;

import java.util.Properties;
import java.util.function.Predicate;

/**
 * Helper methods to deal with configurations
 */
public final class ConfigUtils {
    private ConfigUtils() {
        // utility class
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
