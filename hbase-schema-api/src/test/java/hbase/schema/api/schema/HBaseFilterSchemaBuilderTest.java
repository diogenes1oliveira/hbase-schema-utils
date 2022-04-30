package hbase.schema.api.schema;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class HBaseFilterSchemaBuilderTest {
//    @Test
//    @DisplayName("No filter for columns if no prefixes set")
//    void build_NullColumnFiltersByDefault() {
//        // given a schema without prefixes and a query
//        HBaseFilterSchema<DummyPojo> filterSchema = new HBaseFilterSchemaBuilder<DummyPojo>().build();
//        DummyPojo query = new DummyPojo();
//
//        // shouldn't build a filter for the columns
//        assertThat(filterSchema.buildColumnFilter(query), nullValue());
//    }
//
//    @Test
//    @DisplayName("Builds a list of ColumnPrefixFilter if there are prefixes set")
//    void build_ColumnPrefixFilterIfSet() {
//        // given a schema and a query
//        HBaseFilterSchema<DummyPojo> filterSchema = new HBaseFilterSchemaBuilder<DummyPojo>()
//                .withPrefixes("p", "q")
//                .build();
//        DummyPojo query = new DummyPojo();
//
//        // should have built a filter list with 2 items
//        FilterList filterList = (FilterList) filterSchema.buildColumnFilter(query);
//        assertThat(filterList, notNullValue());
//        List<Filter> filters = filterList.getFilters();
//        assertThat(filters.size(), equalTo(2));
//
//        // each item should be a ColumnPrefixFilter
//        ColumnPrefixFilter filter0 = (ColumnPrefixFilter) filters.get(0);
//        ColumnPrefixFilter filter1 = (ColumnPrefixFilter) filters.get(1);
//
//        // with the configured prefixes
//        assertThat(filter0.getPrefix(), equalTo(new byte[]{'p'}));
//        assertThat(filter1.getPrefix(), equalTo(new byte[]{'q'}));
//    }
//
//    @Test
//    @DisplayName("Builds a MultiRowRangeFilter for the rows filter if a scan key is set")
//    void build_MultiRowRangeFilterIfSet() {
//        // given a schema and two query objects
//        HBaseFilterSchema<DummyPojo> filterSchema = new HBaseFilterSchemaBuilder<DummyPojo>()
//                .withScanKey(DummyPojo::getString, utf8Converter(), 1)
//                .build();
//        DummyPojo query1 = new DummyPojo().withString("p1");
//        DummyPojo query2 = new DummyPojo().withString("q2");
//        DummyPojo query3 = new DummyPojo().withString("t3");
//        DummyPojo query4 = new DummyPojo().withString("u4");
//
//        // should have built a multi row range filter with 2 ranges
//        MultiRowRangeFilter multiRowRangeFilter = (MultiRowRangeFilter) filterSchema.buildRowFilter(
//                asList(query1, query2, query3, query4)
//        );
//        assertThat(multiRowRangeFilter, notNullValue());
//        List<MultiRowRangeFilter.RowRange> ranges = multiRowRangeFilter.getRowRanges();
//        assertThat(ranges.size(), equalTo(2));
//
//        // with the expected ranges
//        assertThat(ranges.get(0).getStartRow(), equalTo(new byte[]{'p'}));
//        assertThat(ranges.get(1).getStartRow(), equalTo(new byte[]{'t'}));
//    }
//
//    @Test
//    @DisplayName("Builds a combined FilterList if no scan key is set")
//    void build_FilterListIfNotSet() {
//        // given a schema and two query objects
//        HBaseFilterSchema<DummyPojo> filterSchema = new HBaseFilterSchemaBuilder<DummyPojo>()
//                .withFilter(pojo -> new ColumnPrefixFilter(("filter=" + pojo.getId()).getBytes(StandardCharsets.UTF_8)))
//                .withScanKey(DummyPojo::getString, utf8Converter(), 1)
//                .build();
//        DummyPojo query0 = new DummyPojo().withId("id0");
//        DummyPojo query1 = new DummyPojo().withId("id1");
//
//        // should have built a filter list with 2 items
//        FilterList filterList = (FilterList) filterSchema.buildRowFilter(asList(query0, query1));
//        assertThat(filterList, notNullValue());
//        List<Filter> filters = filterList.getFilters();
//        assertThat(filters.size(), equalTo(2));
//
//        // each item should be a ColumnPrefixFilter
//        ColumnPrefixFilter filter0 = (ColumnPrefixFilter) filters.get(0);
//        ColumnPrefixFilter filter1 = (ColumnPrefixFilter) filters.get(1);
//
//        // with the configured prefixes
//        assertThat(filter0.getPrefix(), equalTo("filter=id0".getBytes(StandardCharsets.UTF_8)));
//        assertThat(filter1.getPrefix(), equalTo("filter=id1".getBytes(StandardCharsets.UTF_8)));
//    }
}
