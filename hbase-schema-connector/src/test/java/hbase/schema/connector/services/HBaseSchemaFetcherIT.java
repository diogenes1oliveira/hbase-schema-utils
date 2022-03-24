package hbase.schema.connector.services;

import hbase.base.services.PropertiesConfig;
import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseMutator;
import hbase.test.utils.HBaseTestJunitExtension;
import hbase.test.utils.interfaces.HBaseTestInstance;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import testutils.DummyPojo;
import testutils.DummyPojoSchema;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.stream.Stream;

import static hbase.test.utils.HBaseTestHelpers.createTable;
import static hbase.test.utils.HBaseTestHelpers.newTableDescriptor;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

@ExtendWith(HBaseTestJunitExtension.class)
class HBaseSchemaFetcherIT {
    static String family = "f";
    static String tableName;

    DummyPojoSchema schema = new DummyPojoSchema();
    HBaseFetcher<DummyPojo, DummyPojo> fetcher;
    HBaseMutator<DummyPojo> mutator;
    HBaseFactory factory;

    @BeforeAll
    static void setUpTable(TableName tempTable, HBaseTestInstance testInstance, Connection connection) {
        testInstance.cleanUp();
        tableName = tempTable.getNameAsString();
        createTable(connection, newTableDescriptor(tableName, family));
    }

    @BeforeEach
    void setUp(Properties props) {
        this.factory = new HBaseFactory(new PropertiesConfig(props), schema);

        this.fetcher = factory.getFetcher(family, tableName, DummyPojoSchema.class.getSimpleName());
        this.mutator = factory.getMutator(family, tableName, DummyPojoSchema.class.getSimpleName());
    }

    @ParameterizedTest
    @MethodSource("providePojos")
    void mutateAndFetchBack(DummyPojo pojo, DummyPojo query) throws IOException {
        assertThat(fetcher.get(singletonList(query)), empty());
        assertThat(fetcher.scan(singletonList(query)), empty());

        mutator.mutate(singletonList(pojo));

        assertThat(fetcher.get(singletonList(query)), equalTo(singletonList(pojo)));
        assertThat(fetcher.scan(singletonList(query)), equalTo(singletonList(pojo)));
    }

    static Stream<Arguments> providePojos() {
        return Stream.of(
                Arguments.of(
                        new DummyPojo().withInstant(Instant.ofEpochMilli(42_000L))
                                       .withId("some-id")
                                       .withString("some-string")
                                       .withMap1(emptyMap())
                                       .withMap2(emptyMap()),
                        new DummyPojo().withId("some-id")
                )
        );
    }
}
