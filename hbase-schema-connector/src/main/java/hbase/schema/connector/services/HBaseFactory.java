package hbase.schema.connector.services;

import hbase.base.interfaces.Config;
import hbase.connector.services.HBaseConnector;
import hbase.schema.api.interfaces.HBaseMutationMapper;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseMutationBuilder;
import hbase.schema.connector.interfaces.HBaseMutator;
import hbase.schema.connector.interfaces.HBaseQueryBuilder;
import hbase.schema.connector.models.HBaseConfig;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;

import static hbase.schema.connector.utils.HBaseConfigUtils.toKeyValuePairs;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class HBaseFactory {
    private final HBaseConnector connector;
    private byte[] family;
    private List<Pair<String, String>> tablesAndSchemas;
    private final List<HBaseSchema<?, ?>> schemas;

    public HBaseFactory(Config config, HBaseSchema<?, ?>... schemas) {
        this.connector = new HBaseConnector();
        this.schemas = asList(schemas);

        this.connector.configure(config);
    }

    public <Q, R> HBaseFetcher<Q, R> getFetcher(String family, String tableName, String schemaName) {
        byte[] familyBytes = family.getBytes(StandardCharsets.UTF_8);
        HBaseSchema<Q, R> schema = getSchema(schemaName);
        HBaseMutationMapper<Q> mutationMapper = schema.mutationMapper();
        HBaseQueryBuilder<Q> queryBuilder = new HBaseCellsQueryBuilder<>(schema.scanKeySize(), mutationMapper);
        return new HBaseSchemaFetcher<>(tableName, familyBytes, queryBuilder, schema.resultParser(), connector);
    }

    public <Q, R> HBaseFetcher<Q, R> getFetcher(String family, String schemaNamesByTable, Comparator<R> comparator) {
        List<HBaseFetcher<Q, R>> fetchers = toKeyValuePairs(schemaNamesByTable)
                .stream()
                .map(pair -> this.<Q, R>getFetcher(family, pair.getLeft(), pair.getRight()))
                .collect(toList());
        return new HBaseMultiFetcher<>(comparator, fetchers);
    }

    public <T> HBaseMutator<T> getMutator(String family, String tableName, String schemaName) {
        byte[] familyBytes = family.getBytes(StandardCharsets.UTF_8);
        HBaseSchema<T, ?> schema = getSchema(schemaName);
        HBaseMutationBuilder<T> mutationBuilder = new HBaseCellsMutationBuilder<>(schema.mutationMapper());
        return new HBaseSchemaMutator<>(tableName, familyBytes, mutationBuilder, connector);
    }

    public <T> HBaseMutator<T> getMutator(String family, String schemaNamesByTable) {
        List<Pair<String, String>> pairs = toKeyValuePairs(schemaNamesByTable);
        if (pairs.isEmpty()) {
            throw new IllegalArgumentException("No table for mutator");
        }
        Pair<String, String> pair = pairs.get(0);
        return getMutator(family, pair.getLeft(), pair.getRight());
    }

    @SuppressWarnings("unchecked")
    private <Q, R> HBaseSchema<Q, R> getSchema(String schemaName) {
        return (HBaseSchema<Q, R>) schemas.stream()
                                          .filter(s -> s.name().equals(schemaName))
                                          .findFirst()
                                          .orElseThrow(() -> new IllegalArgumentException("No schema named " + schemaName));
    }
}
