package hbase.base.models;

import hbase.base.interfaces.TypeArg;

import java.util.Objects;

/**
 * Type specification for instance types
 */
public class ValueTypeArg implements TypeArg {
    private final Object value;

    /**
     * @param value object value
     */
    public ValueTypeArg(Object value) {
        this.value = value;
    }

    /**
     * Returns {@code true} iff the target type is also a {@code ValueTypeArg} and its object value is equal to this one
     */
    @Override
    public boolean isAssignableTo(TypeArg target) {
        if (!(target instanceof ValueTypeArg)) {
            return false;
        }
        ValueTypeArg other = (ValueTypeArg) target;
        return Objects.equals(this.value, other.value);
    }

    @Override
    public String toString() {
        return Objects.toString(value);
    }

}
