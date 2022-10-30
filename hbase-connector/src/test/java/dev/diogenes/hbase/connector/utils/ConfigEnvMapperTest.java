package dev.diogenes.hbase.connector.utils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ConfigEnvMapperTest {
    @Test
    void testParseEnv() {

        ConfigEnvMapper mapper = new ConfigEnvMapper("HBASE_CONF_");
        Map<String, String> givenEnv = new HashMap<String, String>() {
            {
                put("HBASE_CONF_SOME_PROP", "value");
                put("HBASE_SOME_PROP", "other");
            }
        };

        Properties parsedEnv = mapper.parseEnv(givenEnv);
        assertThat(parsedEnv.getProperty("some.prop"), equalTo("value"));
    }
}
