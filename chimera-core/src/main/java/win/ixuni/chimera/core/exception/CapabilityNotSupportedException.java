package win.ixuni.chimera.core.exception;

import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;

/**
 * Capability not supported exception
 * <p>
 * Thrown when the requested feature is not supported by the driver
 */
public class CapabilityNotSupportedException extends ChimeraException {

    private final Capability capability;
    private final String driverName;

    public CapabilityNotSupportedException(Capability capability, String driverName) {
        super("NotImplemented",
                String.format("The requested feature '%s' is not supported by driver '%s'",
                        capability.name(), driverName),
                501);
        this.capability = capability;
        this.driverName = driverName;
    }

    public Capability getCapability() {
        return capability;
    }

    public String getDriverName() {
        return driverName;
    }
}
