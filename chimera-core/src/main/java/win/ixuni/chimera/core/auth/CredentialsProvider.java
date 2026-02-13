package win.ixuni.chimera.core.auth;

import reactor.core.publisher.Mono;

/**
 * Credentials provider interface
 * <p>
 * Provides S3 credentials from various sources (config files, databases, etc.).
 */
public interface CredentialsProvider {

    /**
     * Retrieve credentials by Access Key ID
     *
     * @param accessKeyId Access Key ID
     * @return the credentials, or empty if not found
     */
    Mono<S3Credentials> getCredentials(String accessKeyId);

    /**
     * Validate whether the credentials are valid
     *
     * @param accessKeyId Access Key ID
     * @param secretAccessKey Secret Access Key
     * @return true if valid
     */
    default Mono<Boolean> validateCredentials(String accessKeyId, String secretAccessKey) {
        return getCredentials(accessKeyId)
                .map(creds -> creds.isEnabled() && creds.getSecretAccessKey().equals(secretAccessKey))
                .defaultIfEmpty(false);
    }
}
