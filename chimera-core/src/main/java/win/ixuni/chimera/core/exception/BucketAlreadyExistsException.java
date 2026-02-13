package win.ixuni.chimera.core.exception;

/**
 * Bucket already exists exception
 */
public class BucketAlreadyExistsException extends ChimeraException {

    public BucketAlreadyExistsException(String bucketName) {
        super("BucketAlreadyExists", "The requested bucket name is not available: " + bucketName, 409);
    }
}
