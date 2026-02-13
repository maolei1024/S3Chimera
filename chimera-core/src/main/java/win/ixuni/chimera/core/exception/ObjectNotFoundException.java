package win.ixuni.chimera.core.exception;

/**
 * Object not found exception
 */
public class ObjectNotFoundException extends ChimeraException {

    public ObjectNotFoundException(String bucketName, String key) {
        super("NoSuchKey", "The specified key does not exist: " + bucketName + "/" + key, 404);
    }
}
