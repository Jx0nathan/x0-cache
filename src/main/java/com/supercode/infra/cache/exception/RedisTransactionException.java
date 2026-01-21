package com.supercode.infra.cache.exception;

/**
 * @author jonathan.ji
 */
public class RedisTransactionException extends RuntimeException {

    public RedisTransactionException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisTransactionException(String message) {
        super(message);
    }

    public RedisTransactionException(Throwable cause) {
        super(cause);
    }

    public RedisTransactionException() {
        super();
    }

    /**
     * for batter performance
     */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    public Throwable doFillInStackTrace() {
        return fillInStackTrace();
    }
}
