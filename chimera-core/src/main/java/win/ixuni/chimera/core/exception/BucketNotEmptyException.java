package win.ixuni.chimera.core.exception;

/**
 * Bucket not empty exception
 * 
 * Thrown when attempting to delete a non-empty bucket.
 */
public class BucketNotEmptyException extends ChimeraException {

    public BucketNotEmptyException(String bucketName) {
        super("BucketNotEmpty", "The bucket you tried to delete is not empty: " + bucketName, 409);
    }
}
