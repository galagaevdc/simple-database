package com.currency.books.exception;

public class DatabaseWriteException extends SimpleDatabaseException {
    public DatabaseWriteException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseWriteException(String message) {
        super(message);
    }
}
