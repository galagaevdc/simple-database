package com.currency.books.exception;

public class TableAlreadyExistsException extends SimpleDatabaseException {
    public TableAlreadyExistsException(final String message) {
        super(message);
    }
}
