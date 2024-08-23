package com.currency.books.utils;

public interface DatabaseConstants {
    String DATABASE_DATA_FOLDER = System.getProperty("java.io.tmpdir") + "/data-folder";
    String INDEX_POST_FIX = "-primary-index.csv";
    String DATA_POSTFIX = "-data.csv";
    String METADATA_POST_FIX = "-metadata.csv";
    String TABLES_FILE = DATABASE_DATA_FOLDER + "/" + "tables.csv";
}
