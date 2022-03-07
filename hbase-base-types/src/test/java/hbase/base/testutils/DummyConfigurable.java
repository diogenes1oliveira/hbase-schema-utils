package hbase.base.testutils;

import hbase.base.interfaces.Config;
import hbase.base.interfaces.Configurable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;

public class DummyConfigurable implements Configurable {
    private final List<String> configs;
    private final Map<String, Integer> integers = new HashMap<>();
    private final Map<String, Double> doubles = new HashMap<>();
    private final Map<String, Properties> properties = new HashMap<>();

    public DummyConfigurable(String... configs) {
        this.configs = asList(configs);
    }

    public List<String> getConfigs() {
        return configs;
    }

    public Map<String, Integer> getIntegers() {
        return integers;
    }

    public Map<String, Double> getDoubles() {
        return doubles;
    }

    public Map<String, Properties> getProperties() {
        return properties;
    }

    @Override
    public Collection<String> configs() {
        return configs;
    }

    @Override
    public void configure(String name, Config config) {
        integers.put(name, config.getValue(name, Integer.class));
    }

    @Override
    public void configureNullable(String name, Config config) {
        doubles.put(name, config.getValue(name, Double.class));
    }

    @Override
    public void configure(String prefix, Properties props) {
        properties.put(prefix, props);
    }
}
