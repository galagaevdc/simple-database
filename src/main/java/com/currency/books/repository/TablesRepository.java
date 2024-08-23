package com.currency.books.repository;

import com.currency.books.exception.DatabaseInitializationException;
import com.currency.books.exception.DatabaseWriteException;
import com.currency.books.exception.TableDoesNotExistException;
import com.currency.books.exception.UnableToReadMetadataException;
import com.currency.books.model.Column;
import com.currency.books.model.ColumnType;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.currency.books.repository.MetadataRepository.COLUMN_NAME_INDEX;
import static com.currency.books.repository.MetadataRepository.COLUMN_PRIMARY_INDEX;
import static com.currency.books.repository.MetadataRepository.COLUMN_TYPE_INDEX;
import static com.currency.books.utils.DatabaseConstants.DATABASE_DATA_FOLDER;
import static com.currency.books.utils.DatabaseConstants.METADATA_POST_FIX;
import static com.currency.books.utils.DatabaseConstants.TABLES_FILE;

@Component
public class TablesRepository {

    private static final int TABLE_NAME_INDEX = 0;
    private static final int LAST_ROW_INDEX = 1;

    @PostConstruct
    public void createTablesFilesIfDoesNotExist() {
        final Path tablesPath = Paths.get(TABLES_FILE);
        if (!Files.exists(tablesPath)) {
            try {
                Files.createFile(tablesPath);
            } catch (IOException e) {
                throw new DatabaseInitializationException("Unable to create tables file", e);
            }
        }
    }

    public synchronized void saveTableNameAndRowIndex(final String tableName) {
        var tablesPath = Paths.get(TABLES_FILE);
        try (final FileWriter fileWriter = new FileWriter(tablesPath.toFile())) {
            final var csvWriter = new CSVWriter(fileWriter);
            csvWriter.writeNext(new String[]{tableName, "0"});
            csvWriter.flush();
        } catch (IOException e) {
            throw new DatabaseWriteException("Unable to save table and row index " + tableName, e);
        }
    }

    public Map<String, Column> readMetadata(String tableName) {
        final Map<String, Column> columnsByName = new HashMap<>();
        final Path metadataPath = Paths.get(getMetadataFileName(tableName));
        if (!Files.exists(metadataPath)) {
            throw new TableDoesNotExistException("Metadata for table " + tableName + " doesn't exist");
        }
        try (final CSVReader reader = new CSVReader(new FileReader(metadataPath.toFile()))) {
            final List<String[]> allRows = reader.readAll();

            for (final String[] row : allRows) {
                final String name = row[COLUMN_NAME_INDEX];
                final String type = row[COLUMN_TYPE_INDEX];
                final String primaryIndex = row[COLUMN_PRIMARY_INDEX];
                columnsByName.put(name, new Column(name,
                        ColumnType.valueOf(type), Boolean.parseBoolean(primaryIndex)));
            }
        } catch (IOException e) {
            throw new UnableToReadMetadataException("Unable to read metadata for table " + tableName, e);
        }
        return columnsByName;
    }

    private static String getMetadataFileName(final String tableName) {
        return DATABASE_DATA_FOLDER + "/" + tableName + METADATA_POST_FIX;
    }
}
