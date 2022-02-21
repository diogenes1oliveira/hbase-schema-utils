package hbase.test.utils.models;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class PrettyEqualsLongMap {
    private final TreeMap<byte[], Long> wrapped;

    public PrettyEqualsLongMap(@Nullable Map<byte[], Long> map) {
        if (map == null) {
            wrapped = null;
        } else {
            wrapped = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            wrapped.putAll(map);
        }
    }

    @Override
    public String toString() {
        if (wrapped == null) {
            return Objects.toString(null);
        }

        Map<String, String> prettyMap = new HashMap<>();
        for (Map.Entry<byte[], Long> entry : wrapped.entrySet()) {
            byte[] key = entry.getKey();
            String prettyKey = Bytes.toStringBinary(key);
            Long value = entry.getValue();
            String prettyValue = Objects.toString(value);
            prettyMap.put(prettyKey, prettyValue);
        }

        return Objects.toString(prettyMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PrettyEqualsLongMap)) {
            return false;
        }
        PrettyEqualsLongMap other = (PrettyEqualsLongMap) obj;

        if ((this.wrapped == null) && (other.wrapped == null)) {
            return true;
        } else if ((this.wrapped == null) != (other.wrapped == null)) {
            return false;
        }
        if (!this.wrapped.keySet().equals(other.wrapped.keySet())) {
            return false;
        }

        for (byte[] key : this.wrapped.keySet()) {
            Long thisValue = this.wrapped.get(key);
            Long otherValue = other.wrapped.get(key);
            if ((thisValue == null) != (otherValue == null)) {
                return false;
            } else if (thisValue != null && !thisValue.equals(otherValue)) {
                return false;
            }
        }
        return true;
    }

    public static PrettyEqualsLongMap prettifyLongMap(@Nullable Map<byte[], Long> map) {
        return new PrettyEqualsLongMap(map);
    }
}
