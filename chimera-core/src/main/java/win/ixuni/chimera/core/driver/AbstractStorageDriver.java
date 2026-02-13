package win.ixuni.chimera.core.driver;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import win.ixuni.chimera.core.config.DriverConfig;

/**
 * Abstract base class for storage drivers
 * <p>
 * Provides common driver functionality; subclasses only need to implement storage-specific logic.
 */
@RequiredArgsConstructor
public abstract class AbstractStorageDriver implements StorageDriver {

    @Getter
    protected final DriverConfig config;

    @Override
    public String getDriverName() {
        return config.getName();
    }
}
