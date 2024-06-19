package ru.nastya.memcache.exception;

public class MemcacheException extends RuntimeException {
    public MemcacheException(String message) {
        super(message);
    }

    public MemcacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
