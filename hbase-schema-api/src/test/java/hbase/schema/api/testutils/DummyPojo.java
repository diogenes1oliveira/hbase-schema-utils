package hbase.schema.api.testutils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class DummyPojo {
    private String id;
    private String field;
    private Long longField;
    private Instant instantField;
    private Boolean booleanField;
    private List<String> listField;
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

    public Boolean getBoolean() {
        return booleanField;
    }

    public void setBoolean(Boolean b) {
        this.booleanField = b;
    }

    public DummyPojo withBoolean(Boolean b) {
        this.booleanField = b;
        return this;
    }

    public List<String> getListField() {
        return listField;
    }

    public void setListField(List<String> listField) {
        this.listField = listField;
    }

    public DummyPojo withListField(List<String> listField) {
        this.listField = listField;
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

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("id", id)
                .append("field", field)
                .append("long", longField)
                .append("instant", instantField)
                .append("boolean", booleanField)
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
                .append(this.longField, other.longField)
                .append(this.instantField, other.instantField)
                .append(this.booleanField, other.booleanField)
                .append(this.map1, other.map1)
                .append(this.map2, other.map2)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(field)
                .append(longField)
                .append(instantField)
                .append(booleanField)
                .append(map1)
                .append(map2)
                .toHashCode();
    }
}
