package com.currency.books.exception;

public class DatabaseInitializationException extends  SimpleDatabaseException{
    public DatabaseInitializationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
