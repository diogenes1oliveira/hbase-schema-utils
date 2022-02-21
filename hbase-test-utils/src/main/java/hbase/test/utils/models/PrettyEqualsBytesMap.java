package hbase.test.utils.models;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class PrettyEqualsBytesMap {
    private final TreeMap<byte[], byte[]> wrapped;

    public PrettyEqualsBytesMap(@Nullable Map<byte[], byte[]> map) {
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
        for (Map.Entry<byte[], byte[]> entry : wrapped.entrySet()) {
            byte[] key = entry.getKey();
            String prettyKey = Bytes.toStringBinary(key);
            byte[] value = entry.getValue();
            String prettyValue = value != null ? Bytes.toStringBinary(value) : Objects.toString(null);
            prettyMap.put(prettyKey, prettyValue);
        }

        return Objects.toString(prettyMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PrettyEqualsBytesMap)) {
            return false;
        }
        PrettyEqualsBytesMap other = (PrettyEqualsBytesMap) obj;

        if ((this.wrapped == null) && (other.wrapped == null)) {
            return true;
        } else if ((this.wrapped == null) != (other.wrapped == null)) {
            return false;
        }
        if (!this.wrapped.keySet().equals(other.wrapped.keySet())) {
            return false;
        }
        for (byte[] key : this.wrapped.keySet()) {
            byte[] thisValue = this.wrapped.get(key);
            byte[] otherValue = other.wrapped.get(key);
            if ((thisValue == null) != (otherValue == null)) {
                return false;
            } else if (thisValue != null && !Bytes.equals(thisValue, otherValue)) {
                return false;
            }
        }
        return true;
    }

    public static PrettyEqualsBytesMap prettifyBytesMap(@Nullable Map<byte[], byte[]> map) {
        return new PrettyEqualsBytesMap(map);
    }
}
