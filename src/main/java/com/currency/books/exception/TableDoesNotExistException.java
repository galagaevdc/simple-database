package com.currency.books.exception;

public class TableDoesNotExistException extends SimpleDatabaseException {
    public TableDoesNotExistException(final String message) {
        super(message);
    }
}
