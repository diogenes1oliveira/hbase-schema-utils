package testutils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.time.Instant;
import java.util.Map;

public class DummyPojo {
    public byte[] bytesField;
    public String stringField;
    public String id;
    public Long longField;
    public Instant instantField;
    private Map<String, String> map1;
    private Map<String, Long> map2;

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

    public Map<String, Long> getMap2() {
        return map2;
    }

    public void setMap2(Map<String, Long> map2) {
        this.map2 = map2;
    }

    public DummyPojo withMap2(Map<String, Long> map2) {
        this.map2 = map2;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("bytesField", bytesField)
                .append("stringField", stringField)
                .append("id", id)
                .append("longField", longField)
                .append("instantField", instantField)
                .append("map1", map1)
                .append("map2", map2)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DummyPojo other = (DummyPojo) o;

        return new EqualsBuilder()
                .append(this.bytesField, other.bytesField)
                .append(this.stringField, other.stringField)
                .append(this.id, other.id)
                .append(this.longField, other.longField)
                .append(this.instantField, other.instantField)
                .append(this.map1, other.map1)
                .append(this.map2, other.map2)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(bytesField)
                .append(stringField)
                .append(id)
                .append(longField)
                .append(instantField)
                .append(map1)
                .append(map2)
                .toHashCode();
    }
}
