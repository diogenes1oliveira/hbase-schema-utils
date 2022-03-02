package hbase.schema.api.testutils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Map;

public class DummyPojo {
    private String id;
    private String field;
    private Long longField;
    private Map<String, String> map1;
    private Map<String, String> map2;

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

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public DummyPojo withField(String field) {
        this.field = field;
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

    public Map<String, String> getMap2() {
        return map2;
    }

    public void setMap2(Map<String, String> map2) {
        this.map2 = map2;
    }

    public DummyPojo withMap2(Map<String, String> map2) {
        this.map2 = map2;
        return this;
    }

    public Long getLong() {
        return longField;
    }

    public void setLong(Long l) {
        this.longField = l;
    }

    public DummyPojo withLong(Long l) {
        this.longField = l;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("id", id)
                .append("field", field)
                .append("map1", map1)
                .append("map2", map2)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        DummyPojo other = (DummyPojo) o;

        return new EqualsBuilder()
                .append(this.id, other.id)
                .append(this.field, other.field)
                .append(this.map1, other.map1)
                .append(this.map2, other.map2)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(field)
                .append(map1)
                .append(map2)
                .toHashCode();
    }
}
