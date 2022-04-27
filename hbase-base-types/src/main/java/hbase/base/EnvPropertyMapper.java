package hbase.base;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.Arrays.asList;


/**
 * Maps environment variables to a Properties object
 * <p>
 * The following rules are applied:
 * <li>Two consecutive underscores are replaced by a dash '-';</li>
 * <li>One underscore is replaced by a dot '.';</li>
 * <li>The result is then converted to lowercase.</li>
 * <li>Three consecutive underscores work as a escape character: the character immediately afterwards
 * is used verbatim.</li>
 * <p>
 * Examples:
 *
 * <li>{@code HBASE_CONF} becomes {@code hbase.conf}</li>
 * <li>{@code HBASE__CONF} becomes {@code hbase-conf}</li>
 * <li>{@code HBASE____CONF} becomes {@code hbase_conf}</li>
 * <li>{@code HBASE___CONF} becomes {@code hbaseConf}</li>
 */
public class EnvPropertyMapper {
    private final Set<String> envPrefixes = new HashSet<>();

    /**
     * @param envPrefixes only environment variables starting with one of these prefixes will be considered
     */
    public EnvPropertyMapper(String... envPrefixes) {
        this.envPrefixes.addAll(asList(envPrefixes));
    }

    /**
     * Builds a properties object corresponding to the environment
     * <p>
     * Environment variables are filtered by prefix and then transformed by the mapping rules
     *
     * @param env environment values
     * @return parsed Properties object
     */
    public Map<String, String> parseEnv(Map<String, String> env) {
        Map<String, String> props = new HashMap<>();

        for (Map.Entry<String, String> entry : env.entrySet()) {
            String envName = entry.getKey();
            if (envPrefixes.stream().anyMatch(envName::startsWith)) {
                String propName = envToPropName(envName);
                String envValue = entry.getValue();

                props.put(propName, envValue);
            }
        }
        return props;
    }

    private static String envToPropName(String envName) {
        String[] parts = envName.split("___");
        StringBuilder builder = new StringBuilder(envToPropNameWithoutEscapes(parts[0]));

        for (int i = 1; i < parts.length; ++i) {
            if (parts[i].isEmpty()) {
                continue;
            }
            char head = parts[i].charAt(0);
            String tail = parts[i].substring(1);
            builder.append(head);
            builder.append(envToPropNameWithoutEscapes(tail));
        }

        return builder.toString();
    }

    private static String envToPropNameWithoutEscapes(String name) {
        return name.toLowerCase(Locale.ROOT)
                   .replace("__", "-")
                   .replace('_', '.');
    }

}
