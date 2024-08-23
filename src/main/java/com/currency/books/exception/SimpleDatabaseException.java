package com.currency.books.exception;

public abstract class SimpleDatabaseException extends RuntimeException {
    public SimpleDatabaseException(final String message) {
        super(message);
    }

    public SimpleDatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
