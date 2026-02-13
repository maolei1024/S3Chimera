package win.ixuni.chimera.core.driver;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.Operation;
import win.ixuni.chimera.core.operation.OperationHandlerRegistry;

/**
 * Storage driver interface V2
 * <p>
 * Command-pattern architecture where all S3 operations are executed via {@link #execute(Operation)}.
 * Each driver implements its own handlers and registers them with the registry.
 */
public interface StorageDriverV2 extends StorageDriver {

        // ==================== Core Methods ====================

        /**
         * Get the operation handler registry
         *
         * @return handler registry
         */
        OperationHandlerRegistry getHandlerRegistry();

        /**
         * Get the driver context
         *
         * @return driver context
         */
        DriverContext getDriverContext();

        /**
         * Execute an operation
         * <p>
         * This is the unified entry point for all S3 operations, with interceptor chain support.
         *
         * @param operation the operation instance
         * @param <O>       operation type
         * @param <R>       return type
         * @return operation result
         */
        default <O extends Operation<R>, R> Mono<R> execute(O operation) {
                return getHandlerRegistry().execute(operation, getDriverContext());
        }

        // ==================== Driver Metadata ====================

        /**
         * Get the driver type identifier
         *
         * @return driver type (e.g. "mysql", "memory")
         */
        String getDriverType();

        /**
         * Get the driver instance name
         *
         * @return instance name (as specified in configuration)
         */
        String getDriverName();

        /**
         * Initialize the driver
         *
         * @return completion signal
         */
        Mono<Void> initialize();

        /**
         * Shut down the driver and release resources
         *
         * @return completion signal
         */
        Mono<Void> shutdown();
}
