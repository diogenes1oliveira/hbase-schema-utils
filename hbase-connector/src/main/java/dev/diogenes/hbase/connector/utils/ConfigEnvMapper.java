package dev.diogenes.hbase.connector.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static dev.diogenes.hbase.connector.utils.ConfigHelpers.mergeProps;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Maps environment variables to a Properties object
 * <p>
 * The following rules are applied:
 * <li>One underscore is replaced by a dot '.';</li>
 * <li>Two consecutive underscores are replaced by a dash '-';</li>
 * <li>The result is then converted to lowercase.</li>
 * <li>Three consecutive underscores work as an escape character: the character
 * immediately afterwards
 * is used verbatim.</li>
 * <p>
 * Examples:
 *
 * <li>{@code HBASE_CONF} becomes {@code hbase.conf}</li>
 * <li>{@code HBASE__CONF} becomes {@code hbase-conf}</li>
 * <li>{@code HBASE____CONF} becomes {@code hbase_conf}</li>
 * <li>{@code HBASE___CONF} becomes {@code hbaseConf}</li>
 */
public class ConfigEnvMapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigEnvMapper.class);

    public static final String ENV_FILE_CONFIG = "CONNECTOR_ENV_FILE";

    private final String envPrefix;

    /**
     * @param envPrefix only environment variables starting with this prefix will be
     *                  considered
     */
    public ConfigEnvMapper(String envPrefix) {
        this.envPrefix = envPrefix;
    }

    /**
     * Builds a properties object corresponding to the environment
     * <p>
     * Environment variables are filtered by prefix and then transformed by the
     * mapping rules
     *
     * @param env environment values
     * @return parsed Properties object
     */
    public Properties parseEnv(Map<String, String> env) {
        Properties props = new Properties();
        String envFilePath = null;

        for (Map.Entry<String, String> entry : env.entrySet()) {
            String envName = entry.getKey();
            if (!envName.startsWith(envPrefix)) {
                continue;
            }
            envName = envName.substring(envPrefix.length());
            String envValue = entry.getValue();

            if (ENV_FILE_CONFIG.equals(envName)) {
                envFilePath = entry.getValue();
            } else {
                String propName = envToPropName(envName);
                props.setProperty(propName, envValue);
            }
        }

        if (!isEmpty(envFilePath)) {
            Properties envProps = parseEnv(envFilePath);
            props = mergeProps(props, envProps);
        }

        return props;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Properties parseEnv(String envFilePath) {
        Properties env = new Properties();

        try (InputStream stream = Files.newInputStream(Paths.get(envFilePath))) {
            env.load(stream);
        } catch (IOException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Failed to load env file " + envFilePath, e);
            } else {
                LOGGER.info("Failed to load env file {}", envFilePath);
            }
        }

        env.remove(ENV_FILE_CONFIG);
        return parseEnv((Map) env);
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
