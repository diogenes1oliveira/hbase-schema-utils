package hbase.schema.connector.services;

import hbase.connector.HBaseConnector;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseMutator;
import hbase.test.utils.HBaseTestJunitExtension;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import testutils.DummyPojo;
import testutils.DummyPojoSchema;

import java.io.IOException;
import java.time.Instant;
import java.util.stream.Stream;

import static hbase.test.utils.HBaseTestHelpers.createTable;
import static hbase.test.utils.HBaseTestHelpers.newTableDescriptor;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith(HBaseTestJunitExtension.class)
public class HBaseFetcherIT {
    HBaseSchema<DummyPojo, DummyPojo> schema = new DummyPojoSchema();
    byte[] family = new byte[]{'f'};
    TableName tempTable;
    HBaseFetcher<DummyPojo, DummyPojo> fetcher;
    HBaseMutator<DummyPojo> mutator;

    @BeforeEach
    void setUp(TableName tempTable, HBaseConnector connector) {
        this.tempTable = tempTable;
        createTable(connector, newTableDescriptor(tempTable, family));

        fetcher = new HBaseSchemaFetcher<>(family, schema, connector);
        mutator = new HBaseSchemaMutator<>(family, schema, connector);
    }

    @ParameterizedTest
    @MethodSource("providePojos")
    void mutateAndFetchBack(DummyPojo pojo, DummyPojo query) throws IOException {
        assertThat(fetcher.get(tempTable, query), nullValue());
        assertThat(fetcher.scan(tempTable, singletonList(query)), empty());

        mutator.mutate(tempTable, pojo);

        assertThat(fetcher.get(tempTable, query), equalTo(pojo));
        assertThat(fetcher.scan(tempTable, singletonList(query)), equalTo(singletonList(pojo)));
    }

    static Stream<Arguments> providePojos() {
        return Stream.of(
                Arguments.of(
                        new DummyPojo().withInstant(Instant.ofEpochMilli(42_000L)).withId("some-id").withString("some-string"),
                        new DummyPojo().withId("some-id")
                )
        );
    }
}
