package hbase.base.interfaces;

import hbase.base.services.ServiceRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static hbase.base.helpers.ConverterUtils.typeArg;
import static hbase.base.interfaces.FromStringConverter.fromStringConverter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("java:S5778")
class FromStringConverterTest {
    FromStringConverter<Integer> adder = fromStringConverter(Integer.class, s -> Integer.parseInt(s) + 10, typeArg("adder"));
    FromStringConverter<Integer> doubler = fromStringConverter(Integer.class, s -> Integer.parseInt(s) * 2, typeArg("doubler"));
    FromStringConverter<Float> floater = fromStringConverter(Float.class, Float::parseFloat, typeArg("floater"));

    @BeforeEach
    void setUp() {
        ServiceRegistry.clear();

        ServiceRegistry.registerService(FromStringConverter.class, adder);
        ServiceRegistry.registerService(FromStringConverter.class, doubler);
        ServiceRegistry.registerService(FromStringConverter.class, floater);
    }

    @Test
    void get_selectsTaggedConverter() {
        FromStringConverter<Integer> selectedAdder = FromStringConverter.get(Integer.class, typeArg("adder"));
        FromStringConverter<Integer> selectDoubler = FromStringConverter.get(Integer.class, typeArg("doubler"));

        assertThat(selectedAdder.convertTo("42"), equalTo(52));
        assertThat(selectDoubler.convertTo("42"), equalTo(84));
    }

    @Test
    void get_ThrowsIfTagsDontMatch() {
        assertThrows(IllegalArgumentException.class, () ->
                FromStringConverter.get(Integer.class, typeArg("other"))
        );
        assertThrows(IllegalArgumentException.class, () ->
                FromStringConverter.get(Integer.class, typeArg("adder"), typeArg("other"))
        );
    }

    @Test
    void stringifiesOutput() {
        assertThat(adder.convertFrom(42), equalTo("42"));
    }

}
