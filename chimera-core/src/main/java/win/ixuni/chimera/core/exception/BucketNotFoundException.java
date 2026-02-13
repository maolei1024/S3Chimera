package win.ixuni.chimera.core.exception;

/**
 * Bucket not found exception
 */
public class BucketNotFoundException extends ChimeraException {

    public BucketNotFoundException(String bucketName) {
        super("NoSuchBucket", "The specified bucket does not exist: " + bucketName, 404);
    }
}
