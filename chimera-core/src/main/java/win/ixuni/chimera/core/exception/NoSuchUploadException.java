package win.ixuni.chimera.core.exception;

/**
 * Multipart upload not found exception
 */
public class NoSuchUploadException extends ChimeraException {

    public NoSuchUploadException(String uploadId) {
        super("NoSuchUpload", "The specified multipart upload does not exist: " + uploadId, 404);
    }
}
