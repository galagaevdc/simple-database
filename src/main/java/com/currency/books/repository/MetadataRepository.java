package com.currency.books.repository;

import com.currency.books.exception.DatabaseWriteException;
import com.currency.books.exception.TableAlreadyExistsException;
import com.currency.books.exception.TableDoesNotExistException;
import com.currency.books.exception.UnableToReadMetadataException;
import com.currency.books.model.Column;
import com.currency.books.model.ColumnType;
import com.currency.books.utils.MultiThreadCsvPrinter;
import com.opencsv.CSVReader;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.currency.books.utils.DatabaseConstants.DATABASE_DATA_FOLDER;
import static com.currency.books.utils.DatabaseConstants.METADATA_POST_FIX;

@Component
public class MetadataRepository {

    static final int COLUMN_NAME_INDEX = 0;
    static final int COLUMN_TYPE_INDEX = 1;
    static final int COLUMN_PRIMARY_INDEX = 2;

    public void saveTableMetadata(final String tableName,
                                  final List<Column> columns) {
        // TODO: implement metadata validation only one column can be primary index
        var metadataPath = Paths.get(getMetadataFileName(tableName));
        if (Files.exists(metadataPath)) {
            throw new TableAlreadyExistsException("Metadata for table " + tableName + " already exists");
        }
        // TODO: Apply try-with-resources where possible
        try (final MultiThreadCsvPrinter metadataPrinter =
                     new MultiThreadCsvPrinter(metadataPath, 0)) {
            for (final Column column : columns) {
                var rowData = new String[3];
                rowData[COLUMN_NAME_INDEX] = column.getColumnName();
                rowData[COLUMN_TYPE_INDEX] = column.getColumnType().name();
                rowData[COLUMN_PRIMARY_INDEX] = String.valueOf(column.isPrimaryIndex());
                metadataPrinter.writeLine(rowData);
            }

            metadataPrinter.flush();
        } catch (IOException e) {
            throw new DatabaseWriteException("Unable to save metadata for table " + tableName, e);
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
