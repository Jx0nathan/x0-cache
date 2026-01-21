package com.supercode.infra.cache.exception;

/**
 * @author jonathan.ji
 */
public class RedisLockException extends RuntimeException {

    public RedisLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisLockException(String message) {
        super(message);
    }

    public RedisLockException(Throwable cause) {
        super(cause);
    }

    public RedisLockException() {
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
