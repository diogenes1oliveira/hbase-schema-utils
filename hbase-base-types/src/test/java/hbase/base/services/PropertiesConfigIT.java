package hbase.base.services;

import hbase.base.interfaces.FromStringConverter;
import hbase.base.testutils.DummyConfigurable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static hbase.base.interfaces.FromStringConverter.fromStringConverter;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

class PropertiesConfigIT {
    @BeforeEach
    void setUp() {
        ServiceRegistry.clear();
        ServiceRegistry.registerService(FromStringConverter.class, fromStringConverter(
                Integer.class, Integer::parseInt
        ));
        ServiceRegistry.registerService(FromStringConverter.class, fromStringConverter(
                Double.class, Double::parseDouble
        ));
    }

    @Test
    void configure_SetsValues() {
        DummyConfigurable configurable = new DummyConfigurable("someInt", "someString.", "someDouble?");
        PropertiesConfig propertiesConfig = new PropertiesConfig(new Properties() {{
            setProperty("someInt", "42");
            setProperty("someDouble", "3.14");
            setProperty("k2", "other");
        }});

        propertiesConfig.configure(configurable);

        assertThat(configurable.getIntegers(), equalTo(singletonMap("someInt", 42)));
        assertThat(configurable.getDoubles(), equalTo(singletonMap("someDouble", 3.14)));
        assertThat(configurable.getProperties().get("someString."), equalTo(new Properties()));
    }

    @Test
    void configure_SetsPrefixes() {
        DummyConfigurable configurable = new DummyConfigurable("prefix.");
        PropertiesConfig propertiesConfig = new PropertiesConfig(new Properties() {{
            setProperty("prefix", "ignored");
            setProperty("prefix.", "empty");
            setProperty("prefix.key", "value");
            setProperty("k2", "other");
        }});

        propertiesConfig.configure(configurable);

        assertThat(configurable.getIntegers().entrySet(), empty());
        assertThat(configurable.getDoubles().entrySet(), empty());
        assertThat(configurable.getProperties(), equalTo(singletonMap("prefix.", new Properties(){{
            setProperty("", "empty");
            setProperty("key", "value");
        }})));
    }
}
