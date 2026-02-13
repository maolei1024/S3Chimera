package win.ixuni.chimera.core.exception;

/**
 * Driver not found exception
 */
public class DriverNotFoundException extends ChimeraException {

    public DriverNotFoundException(String driverName) {
        super("DriverNotFound", "The specified driver does not exist: " + driverName, 500);
    }
}
