package win.ixuni.chimera.core.driver;

import lombok.Getter;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.OperationHandlerRegistry;

import java.util.Set;

/**
 * Abstract base class for V2 storage drivers
 * <p>
 * Provides common implementation; subclasses only need to implement initialization and register handlers.
 * All S3 operations are executed via the {@code execute(Operation)} method.
 */
public abstract class AbstractStorageDriverV2 implements StorageDriverV2 {

    @Getter
    protected final OperationHandlerRegistry handlerRegistry = new OperationHandlerRegistry();

    @Override
    public Set<Capability> getCapabilities() {
        // Aggregated from all registered handler-declared capabilities
        return handlerRegistry.getAggregatedCapabilities();
    }
}
