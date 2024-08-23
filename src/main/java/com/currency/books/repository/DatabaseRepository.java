package com.currency.books.repository;

import com.currency.books.exception.DatabaseInitializationException;
import com.currency.books.exception.DatabaseReadException;
import com.currency.books.exception.DatabaseWriteException;
import com.currency.books.exception.IndexIsNotSupportedException;
import com.currency.books.exception.TableAlreadyExistsException;
import com.currency.books.exception.TableDoesNotExistException;
import com.currency.books.exception.TypeMismatchException;
import com.currency.books.model.Column;
import com.currency.books.model.ColumnData;
import com.currency.books.model.Row;
import com.currency.books.utils.DatabaseConstants;
import com.currency.books.utils.MultiThreadCsvPrinter;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DatabaseRepository {
    private final MetadataRepository metadataRepository;
    private final Map<String, Map<Long, Integer>> indexesByTableName = new ConcurrentHashMap<>();
    private final Map<String, MultiThreadCsvPrinter> dataWritersByTableName = new ConcurrentHashMap<>();
    private final Map<String, MultiThreadCsvPrinter> indexWritersByTableName = new ConcurrentHashMap<>();

    public DatabaseRepository(final MetadataRepository metadataRepository) {
        this.metadataRepository = metadataRepository;
    }

    @PostConstruct
    public void setupDatabaseDataFolder() {
        Path path = Paths.get(DatabaseConstants.DATABASE_DATA_FOLDER);
        if (!Files.exists(path)) {
            try {
                Files.createDirectory(path);
            } catch (IOException e) {
                throw new DatabaseInitializationException("Unable to create database data folder", e);
            }
        }

        // iterate over files in directory and init data writers
        // TODO: init indexes
        // TODO: init data writers
        // TODO: init index writers
    }


    public void createTable(final String tableName, final List<Column> columns) {
        final Path dataPath = Paths.get(getDataFileName(tableName));
        final Path indexPath = Paths.get(getIndexFileName(tableName));

        if (Files.exists(dataPath)) {
            throw new TableAlreadyExistsException("Table " + tableName + " already exists");
        }

        metadataRepository.saveTableMetadata(tableName, columns);
        // save tableName to tables.csv


        try {
            dataWritersByTableName.put(tableName, new MultiThreadCsvPrinter(dataPath, 0));
            indexWritersByTableName.put(tableName, new MultiThreadCsvPrinter(indexPath, 0));
            indexesByTableName.put(tableName, new HashMap<>());
        } catch (IOException e) {
            throw new DatabaseWriteException("Unable to create writer for table " + tableName, e);
        }
    }

    private static String getDataFileName(final String tableName) {
        return DatabaseConstants.DATABASE_DATA_FOLDER + "/" + tableName + DatabaseConstants.DATA_POSTFIX;
    }

    private static String getIndexFileName(final String tableName) {
        return DatabaseConstants.DATABASE_DATA_FOLDER + "/" + tableName + DatabaseConstants.INDEX_POST_FIX;
    }

    //TODO: Consider atomicity issues. For example, data is saved, but index is not updated
    public void insertInto(final String tableName, final List<Row> rows) {
        final Map<String, Column> columnsByName = metadataRepository.readMetadata(tableName);
        saveData(tableName, rows, columnsByName);
    }

    public Optional<Row> selectById(final String tableName, final Long primaryKey) {
        final var indexMap = indexesByTableName.get(tableName);
        if (indexMap == null) {
            throw new TableDoesNotExistException("Unable to find index for table " + tableName);
        }
        final var rowIndex = indexMap.get(primaryKey);
        if (rowIndex == null) {
            return Optional.empty();
        }
        try {
            final var metadata = new ArrayList<>(metadataRepository.
                    readMetadata(tableName).values());
            String[] readLine = readLine(tableName, rowIndex);
            List<ColumnData> rowData = new ArrayList<>();
            for (int i = 0; i < readLine.length; i++) {
                final String data = readLine[i];
                final var columMetadata = metadata.get(i);

                final ColumnData columnData = new ColumnData();
                columnData.setColumnName(columMetadata.getColumnName());
                columnData.setData(columMetadata.getColumnType().parse(data));
                rowData.add(columnData);
            }
            return Optional.of(new Row(rowData));
        } catch (IOException e) {
            throw new DatabaseReadException("Unable to read data for table " + tableName, e);
        }
    }

    public void closeWriters() {
        try {
            for (final MultiThreadCsvPrinter multiThreadCsvPrinter : dataWritersByTableName.values()) {
                multiThreadCsvPrinter.close();
            }
            for (final MultiThreadCsvPrinter multiThreadCsvPrinter : indexWritersByTableName.values()) {
                multiThreadCsvPrinter.close();
            }
        } catch (IOException e) {
            throw new DatabaseWriteException("Unable to close writers", e);
        }
    }

    private static String[] readLine(final String tableName, final int rowIndex) throws IOException {
        final CSVReader reader = new CSVReader(new FileReader(getDataFileName(tableName)),
                CSVParser.DEFAULT_SEPARATOR, CSVParser.DEFAULT_QUOTE_CHARACTER, rowIndex);
        final var line = reader.readNext();
        reader.close();
        return line;
    }

    private void saveData(String tableName, List<Row> rows, Map<String, Column> columnsByName) {
        try {
            final var indexWriter = this.indexWritersByTableName.get(tableName);
            final var dataWriter = this.dataWritersByTableName.get(tableName);
            final var index = this.indexesByTableName.get(tableName);

            if (indexWriter == null) {
                throw new DatabaseWriteException("Unable to find index writer for table " + tableName);
            }

            if (dataWriter == null) {
                throw new DatabaseWriteException("Unable to find data writer for table " + tableName);
            }

            for (final Row row : rows) {
                final List<ColumnData> columnDataByRows = row.getColumns();
                final List<String> rowData = new ArrayList<>();
                final Long primaryIndex = prepareRow(columnsByName, columnDataByRows, rowData);
                if (primaryIndex == null) {
                    throw new IndexIsNotSupportedException("Index should be defined for row");
                }

                final Integer rowIndex = dataWriter.writeLine(convertListToArray(rowData));

                indexWriter.writeLine(String.valueOf(primaryIndex), String.valueOf(rowIndex));
                index.put(primaryIndex, rowIndex);
            }

            indexWriter.flush();
            dataWriter.flush();
        } catch (IOException e) {
            throw new DatabaseWriteException("Unable to save data for table " + tableName, e);
        }
    }

    private static Long prepareRow(final Map<String, Column> columnsByName,
                                   final List<ColumnData> columnDataByRows, final List<String> rowData) {
        Long primaryIndex = null;
        for (final ColumnData columnData : columnDataByRows) {
            final Column column = columnsByName.get(columnData.getColumnName());
            if (!columnData.getData().getClass().equals(column.getColumnType().getClazz())) {
                throw new TypeMismatchException("Column " + columnData.getColumnName() + " must have " + column.getColumnType().toString() + " type");
            }
            if (column.isPrimaryIndex()) {
                if (!(columnData.getData() instanceof Long)) {
                    throw new IndexIsNotSupportedException("Currently only Long is possible type for index");
                }
                primaryIndex = (Long) columnData.getData();
            }
            rowData.add(String.valueOf(columnData.getData()));
        }
        return primaryIndex;
    }

    public static String[] convertListToArray(List<String> list) {
        String[] array = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

}
