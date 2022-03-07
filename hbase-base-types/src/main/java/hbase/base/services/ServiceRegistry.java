package hbase.base.services;

import hbase.base.interfaces.Service;

import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static java.util.Comparator.comparing;

/**
 * Global registry for all {@link Service} implementations
 */
@SuppressWarnings("unchecked")
public final class ServiceRegistry {
    private static final Map<Class<?>, Set<?>> instancesByType = new ConcurrentHashMap<>();

    private ServiceRegistry() {
        // utility class
    }

    /**
     * Gets an instance of the service that matches the given predicate
     * <p>
     * The services are loaded via {@link ServiceLoader} the first time the service type is requested
     *
     * @param serviceType service interface class instance
     * @param selector    selects the appropriate service
     * @param <T>         service interface type
     * @return service with the highest priority
     * @throws IllegalArgumentException no matching service was found
     */
    public static <T extends Service> T findService(Class<T> serviceType, Predicate<T> selector) {
        return getCurrent(serviceType)
                .stream()
                .filter(selector)
                .max(comparing(Service::priority))
                .orElseThrow(() -> new IllegalArgumentException("No such service"));
    }

    /**
     * Dynamically add a new service instance to the registry
     * <p>
     * The services are loaded via {@link ServiceLoader} the first time the service type is requested
     *
     * @param serviceType service interface class instance
     * @param service     new instance to register
     * @param <T>         service interface type
     */
    public static <T extends Service> void registerService(Class<T> serviceType, T service) {
        Set<T> newServices = new HashSet<>(getCurrent(serviceType));
        newServices.add(service);
        instancesByType.put(serviceType, newServices);
    }

    /**
     * Clears all currently loaded services
     */
    public static void clear() {
        instancesByType.clear();
    }

    private static <T extends Service> Set<T> getCurrent(Class<T> serviceType) {
        return (Set<T>) instancesByType.computeIfAbsent(serviceType, type -> {
            ServiceLoader<T> loader = ServiceLoader.load(serviceType);
            Set<T> services = new HashSet<>();
            loader.forEach(services::add);
            return services;
        });
    }
}
