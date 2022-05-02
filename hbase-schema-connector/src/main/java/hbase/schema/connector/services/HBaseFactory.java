package hbase.schema.connector.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseFactory.class);
//
//    private final HBaseConnector connector;
//    private final List<HBaseSchema<?, ?>> schemas = new ArrayList<>();
//
//    public HBaseFactory(Config config, HBaseSchema<?, ?>... schemas) {
//        this(config, asList(schemas));
//    }
//
//    public HBaseFactory(Config config, Iterable<HBaseSchema<?, ?>> schemas) {
//        this.connector = new HBaseConnector();
//
//        List<String> names = new ArrayList<>();
//        schemas.forEach(schema -> {
//            this.schemas.add(schema);
//            names.add(schema.name());
//        });
//        LOGGER.info("Registered schemas: {}", names);
//
//
//        this.connector.configure(config);
//    }

//    public HBaseConnector getConnector() {
//        return connector;
//    }
//
//    public <Q, R> HBaseFetcher<Q, R> getFetcher(String family, String tableName, String schemaName) {
//        byte[] familyBytes = family.getBytes(StandardCharsets.UTF_8);
//        HBaseSchema<Q, R> schema = getSchema(schemaName);
//        HBaseFilterBuilder<Q> filterBuilder = new HBaseCellsFilterBuilder<>(schema.queryMapper());
//        return new HBaseSchemaFetcher<>(tableName, familyBytes, filterBuilder, schema.resultParser(), connector::context);
//    }
//
//    public <Q, R> HBaseFetcher<Q, R> getFetcher(String family, String schemaNamesByTable) {
//        List<HBaseFetcher<Q, R>> fetchers = toKeyValuePairs(schemaNamesByTable)
//                .stream()
//                .map(pair -> this.<Q, R>getFetcher(family, pair.getLeft(), pair.getRight()))
//                .collect(toList());
//        return new HBaseMultiFetcher<>(fetchers);
//    }
//
//    public <T> HBaseMutator<T> getMutator(String family, String tableName, String schemaName) {
//        byte[] familyBytes = family.getBytes(StandardCharsets.UTF_8);
//        HBaseSchema<T, ?> schema = getSchema(schemaName);
//        HBaseMutationBuilder<T> mutationBuilder = new HBaseCellsMutationBuilder<>(schema.mutationMapper());
//        return new HBaseSchemaMutator<>(tableName, familyBytes, mutationBuilder, connector::context);
//    }
//
//    public <T> HBaseMutator<T> getMutator(String family, String schemaNamesByTable) {
//        List<Pair<String, String>> pairs = toKeyValuePairs(schemaNamesByTable);
//        if (pairs.isEmpty()) {
//            throw new IllegalArgumentException("No table for mutator");
//        }
//        Pair<String, String> pair = pairs.get(0);
//        return getMutator(family, pair.getLeft(), pair.getRight());
//    }
//
//    @SuppressWarnings("unchecked")
//    private <Q, R> HBaseSchema<Q, R> getSchema(String schemaName) {
//        return (HBaseSchema<Q, R>) schemas.stream()
//                                          .filter(s -> s.name().equals(schemaName))
//                                          .findFirst()
//                                          .orElseThrow(() -> new IllegalArgumentException("No schema named " + schemaName));
//    }
}
