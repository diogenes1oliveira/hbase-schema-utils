package hbase.base.services;

import hbase.base.testutils.DummyService;
import hbase.base.testutils.DummyService1;
import hbase.base.testutils.DummyService2;
import org.junit.jupiter.api.Test;
import otherpackage.DummyServiceFromOtherPackage;

import java.util.List;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServiceRegistryIT {
    @Test
    void findService_PrioritizesOtherPackages() {
        DummyService service = ServiceRegistry.findService(DummyService.class, s -> true);
        assertThat(service, instanceOf(DummyServiceFromOtherPackage.class));
    }

    @Test
    void findService_PrioritizesExplicitPriority() {
        List<String> excluded = asList("DummyServiceFromOtherPackage", "DummyService2");
        DummyService service = ServiceRegistry.findService(
                DummyService.class,
                s -> !excluded.contains(s.getClass().getSimpleName())
        );
        assertThat(service, instanceOf(DummyService1.class));
    }

    @Test
    void findService_PrioritizesHigherPriority() {
        List<String> excluded = asList("DummyServiceFromOtherPackage", "DummyService0");
        DummyService service = ServiceRegistry.findService(
                DummyService.class,
                s -> !excluded.contains(s.getClass().getSimpleName())
        );
        assertThat(service, instanceOf(DummyService2.class));
    }

    @Test
    void findService_ThrowsWhenNoService() {
        Predicate<DummyService> testFalse = s -> false;
        assertThrows(IllegalArgumentException.class, () ->
                ServiceRegistry.findService(DummyService.class, testFalse)
        );
    }

    @Test
    void registerService_AddsDynamically() {
        Predicate<DummyService> trueTest = s -> true;

        // should return the highest priority from META-INF/services before registering
        DummyService serviceBefore = ServiceRegistry.findService(DummyService.class, trueTest);
        assertThat(serviceBefore, instanceOf(DummyServiceFromOtherPackage.class));

        // do register
        SomeSmartService someSmartService = new SomeSmartService();
        ServiceRegistry.registerService(DummyService.class, someSmartService);

        // should return the new instance
        DummyService serviceAfter = ServiceRegistry.findService(DummyService.class, trueTest);
        assertThat(serviceAfter, sameInstance(someSmartService));
    }

    static class SomeSmartService implements DummyService {
        @Override
        public int priority() {
            return 101;
        }
    }
}
