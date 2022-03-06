package hbase.test.utils.models;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Wraps over a {@code Map<byte[], byte[]>}, providing a printable {@link Object#toString()} and a
 * proper {@link Object#equals(Object)} implementation
 */
public class PrettyBytesMap {
    private final TreeMap<byte[], byte[]> wrapped;

    /**
     * @param map input bytes map
     */
    public PrettyBytesMap(@Nullable Map<byte[], byte[]> map) {
        if (map == null) {
            wrapped = null;
        } else {
            wrapped = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            wrapped.putAll(map);
        }
    }

    /**
     * Original map passed to the constructor
     */
    @Nullable
    public SortedMap<byte[], byte[]> getWrapped() {
        return wrapped;
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
        if (!(obj instanceof PrettyBytesMap)) {
            return false;
        }
        PrettyBytesMap other = (PrettyBytesMap) obj;
        if (this.wrapped == null) {
            return other.wrapped == null;
        } else if(other.wrapped == null) {
            return false;
        }

        if (this.wrapped.size() != other.wrapped.size()) {
            return false;
        }

        EqualsBuilder builder = new EqualsBuilder();
        for (Map.Entry<byte[], byte[]> entry : this.wrapped.entrySet()) {
            byte[] key = entry.getKey();
            byte[] thisValue = entry.getValue();
            byte[] otherValue = other.wrapped.get(key);
            builder = builder.append(thisValue, otherValue);
        }

        return builder.isEquals();
    }

    @Override
    public int hashCode() {
        if (wrapped == null) {
            return Objects.hashCode(null);
        }
        Map<String, String> hashable = new HashMap<>();

        for (Map.Entry<byte[], byte[]> entry : wrapped.entrySet()) {
            String key = Base64.getEncoder().encodeToString(entry.getKey());
            String value = Base64.getEncoder().encodeToString(entry.getValue());
            hashable.put(key, value);
        }

        return Objects.hashCode(hashable);
    }
}
