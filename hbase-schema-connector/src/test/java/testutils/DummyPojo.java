package testutils;

import java.time.Instant;
import java.util.Map;

public class DummyPojo {
    public byte[] bytesField;
    public String stringField;
    public String id;
    public Long longField;
    public Instant instantField;
    private Map<String, String> map1;

    public byte[] getBytes() {
        return bytesField;
    }

    public void setBytes(byte[] bytes) {
        this.bytesField = bytes;
    }

    public DummyPojo withBytes(byte[] bytes) {
        this.bytesField = bytes;
        return this;
    }

    public String getString() {
        return stringField;
    }

    public void setString(String s) {
        this.stringField = s;
    }

    public DummyPojo withString(String s) {
        this.stringField = s;
        return this;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public DummyPojo withId(String id) {
        this.id = id;
        return this;
    }

    public Long getLong() {
        return longField;
    }

    public void setLong(long l) {
        this.longField = l;
    }

    public DummyPojo withLong(long l) {
        this.longField = l;
        return this;
    }

    public Instant getInstant() {
        return instantField;
    }

    public void setInstant(Instant instant) {
        this.instantField = instant;
    }

    public DummyPojo withInstant(Instant instant) {
        this.instantField = instant;
        return this;
    }

    public Map<String, String> getMap1() {
        return map1;
    }

    public void setMap1(Map<String, String> map1) {
        this.map1 = map1;
    }

    public DummyPojo withMap1(Map<String, String> map1) {
        this.map1 = map1;
        return this;
    }

}
