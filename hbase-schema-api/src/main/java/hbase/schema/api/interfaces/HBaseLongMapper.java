package hbase.schema.api.interfaces;

import java.util.function.Function;

@FunctionalInterface
public interface HBaseLongMapper<T> {
    Long toLong(T value);

    default HBaseLongMapper<T> andThen(Function<Long, Long> mapper) {
        return value -> {
            Long l = this.toLong(value);
            if (l == null) {
                return null;
            } else {
                return mapper.apply(l);
            }
        };
    }

    static <T> HBaseLongMapper<T> singleton(Long value) {
        return t -> value;
    }
}
