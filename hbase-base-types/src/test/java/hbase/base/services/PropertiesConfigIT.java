package hbase.base.services;

import hbase.base.interfaces.FromStringConverter;
import hbase.base.testutils.DummyConfigurable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static hbase.base.interfaces.FromStringConverter.fromStringConverter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PropertiesConfigIT {
    @BeforeEach
    void setUp() {
        ServiceRegistry.clear();
    }

    @Test
    void configure_SetsValues() {
        DummyConfigurable configurable = new DummyConfigurable();
        PropertiesConfig propertiesConfig = new PropertiesConfig(new Properties() {{
            setProperty("some.string", "some value");
            setProperty("some.double", "3.14");
        }});

        // before registering the converters
        assertThrows(IllegalArgumentException.class, () -> configurable.configure(propertiesConfig));

        ServiceRegistry.registerService(FromStringConverter.class, fromStringConverter(
                Integer.class, Integer::parseInt
        ));
        ServiceRegistry.registerService(FromStringConverter.class, fromStringConverter(
                Double.class, Double::parseDouble
        ));

        configurable.configure(propertiesConfig);

        assertThat(configurable.getString(), equalTo("some value"));
        assertThat(configurable.getDouble(), equalTo(3.14));
        assertThat(configurable.getInt(), equalTo(-1));
    }

}
