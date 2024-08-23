package com.currency.books.exception;

public class UnableToReadMetadataException extends SimpleDatabaseException {
    public UnableToReadMetadataException(String message, Throwable cause) {
        super(message, cause);
    }
}
