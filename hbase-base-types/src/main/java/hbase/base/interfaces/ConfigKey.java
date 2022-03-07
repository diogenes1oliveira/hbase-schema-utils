package hbase.base.interfaces;

public interface ConfigKey {
    String key();

    <T> T fromConfig(Config config);
}
