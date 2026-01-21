package com.supercode.infra.cache.exception;

/**
 * @author jonathan
 */
public class RedisInfraException extends RuntimeException {

    public RedisInfraException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisInfraException(String message) {
        super(message);
    }

    public RedisInfraException(Throwable cause) {
        super(cause);
    }

    public RedisInfraException() {
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
