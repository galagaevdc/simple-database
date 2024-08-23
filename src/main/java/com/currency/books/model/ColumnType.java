package com.currency.books.model;

public enum ColumnType {
    LONG {
        @Override
        public Class getClazz() {
            return Long.class;
        }

        @Override
        public Object parse(final String data) {
            return Long.parseLong(data);
        }
    }, STRING {
        @Override
        public Class getClazz() {
            return String.class;
        }

        @Override
        public Object parse(final String data) {
            return data;
        }
    };

    public abstract Class getClazz();

    public abstract Object parse(final String data);
}
