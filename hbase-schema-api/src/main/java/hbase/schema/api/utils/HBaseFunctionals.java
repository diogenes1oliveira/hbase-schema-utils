package hbase.schema.api.utils;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class HBaseFunctionals {
    private HBaseFunctionals() {
        // utility class
    }

    public static <T, U> BiConsumer<T, U> dummyBiConsumer() {
        return (t, u) -> {

        };
    }

    public static <T, V> Function<T, V> fixedFunction(V value) {
        return obj -> value;
    }


    public static <K1, V1, K2 extends Comparable<K2>, V2> TreeMap<K2, V2> mapToTreeMap(Map<K1, V1> input,
                                                                                       Function<K1, K2> keyMapper,
                                                                                       Function<V1, V2> valueMapper) {
        TreeMap<K2, V2> result = new TreeMap<>();

        for (Map.Entry<K1, V1> entry : input.entrySet()) {
            K2 k = keyMapper.apply(entry.getKey());
            V2 v = valueMapper.apply(entry.getValue());
            result.put(k, v);
        }

        return result;
    }

    public static <K1, V1, K2, V2> TreeMap<K2, V2> mapToTreeMap(Map<K1, V1> input,
                                                                Function<K1, K2> keyMapper,
                                                                Function<V1, V2> valueMapper,
                                                                Comparator<K2> comparator) {
        TreeMap<K2, V2> result = new TreeMap<>(comparator);

        for (Map.Entry<K1, V1> entry : input.entrySet()) {
            K2 k = keyMapper.apply(entry.getKey());
            V2 v = valueMapper.apply(entry.getValue());
            result.put(k, v);
        }

        return result;
    }


}
