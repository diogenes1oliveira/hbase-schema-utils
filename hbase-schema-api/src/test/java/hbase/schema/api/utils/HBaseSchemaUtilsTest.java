package hbase.schema.api.utils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HBaseSchemaUtilsTest {
    byte[] B1 = new byte[]{'1'};
    byte[] B2 = new byte[]{'2'};
    byte[] B3 = new byte[]{'3'};
    byte[] B4 = new byte[]{'4'};

    @SuppressWarnings("PrimitiveArrayArgumentToVarargsMethod")
    @Test
    void asBytesTreeMap_ValidatesSize() {
        assertThrows(IllegalArgumentException.class, () ->
                HBaseSchemaUtils.asBytesTreeMap(B1, B2, B3)
        );
    }

    @Test
    void asBytesTreeMap_ValidatesType() {
        Object dummy = new Object();
        assertThrows(ClassCastException.class, () ->
                HBaseSchemaUtils.asBytesTreeMap(B1, B2, B3, null, B4, dummy)
        );
    }

    @Test
    void verifyNonNull_ThrowsIfNoObject() {
        assertThrows(IllegalStateException.class, () ->
                HBaseSchemaUtils.verifyNonNull("msg")
        );
    }

    @Test
    void verifyNonNull_ThrowsIfNoNotNullObject() {
        assertThrows(IllegalStateException.class, () ->
                HBaseSchemaUtils.verifyNonNull("msg", null, null)
        );
    }

    @Test
    void verifyNonEmpty_ThrowsIfNoCollection() {
        assertThrows(IllegalStateException.class, () ->
                HBaseSchemaUtils.verifyNonEmpty("msg")
        );
    }

    @Test
    void verifyNonEmpty_ThrowsIfNoNotEmptyObject() {
        List<?> empty = emptyList();
        assertThrows(IllegalStateException.class, () ->
                HBaseSchemaUtils.verifyNonEmpty("msg", empty, null)
        );
    }
}
