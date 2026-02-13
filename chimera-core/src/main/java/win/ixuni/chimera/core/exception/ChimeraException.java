package win.ixuni.chimera.core.exception;

import lombok.Getter;

/**
 * S3Chimera base exception
 */
@Getter
public class ChimeraException extends RuntimeException {

    private final String errorCode;
    private final int httpStatus;

    public ChimeraException(String errorCode, String message, int httpStatus) {
        super(message);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }

    public ChimeraException(String errorCode, String message, int httpStatus, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }
}
