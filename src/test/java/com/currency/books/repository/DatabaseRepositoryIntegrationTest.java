package com.currency.books.repository;

import com.currency.books.model.Column;
import com.currency.books.model.ColumnData;
import com.currency.books.model.ColumnType;
import com.currency.books.model.Row;
import com.currency.books.utils.DatabaseConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = {DatabaseRepository.class, MetadataRepository.class})
class DatabaseRepositoryIntegrationTest {

    private static final String TEST_TABLE = "test_table";
    private static final Long FIRST_ID = 13L;
    private static final String FIRST_DATA = "test1";
    private static final Long SECOND_ID = 32L;
    private static final String SECOND_DATA = "test2";
    private static final String ID_COLUMN = "id";
    private static final String VALUE_COLUMN = "value";

    @Autowired
    private DatabaseRepository databaseRepository;


    @Test
    public void testBasicFlow() throws IOException {
        try {
            final var metadata = new ArrayList<Column>();
            metadata.add(new Column(ID_COLUMN, ColumnType.LONG, true));
            metadata.add(new Column(VALUE_COLUMN, ColumnType.STRING, false));

            databaseRepository.createTable(TEST_TABLE, metadata);
            final List<Row> rows = new ArrayList<>();
            rows.add(new Row(List.of(new ColumnData(ID_COLUMN, FIRST_ID),
                    new ColumnData(VALUE_COLUMN, FIRST_DATA))));
            rows.add(new Row(List.of(new ColumnData(ID_COLUMN, SECOND_ID),
                    new ColumnData(VALUE_COLUMN, SECOND_DATA))));
            databaseRepository.insertInto(TEST_TABLE, rows);

            final var optionalRow = databaseRepository.selectById(TEST_TABLE, SECOND_ID);

            assertTrue(optionalRow.isPresent());
            assertEquals(SECOND_ID, optionalRow.get().getColumns().get(0).getData());
            assertEquals(ID_COLUMN, optionalRow.get().getColumns().get(0).getColumnName());
            assertEquals(VALUE_COLUMN, optionalRow.get().getColumns().get(1).getColumnName());
            assertEquals(SECOND_DATA, optionalRow.get().getColumns().get(1).getData());

        } finally {
            databaseRepository.closeWriters();
            cleanUpFolders();
        }
    }

    @BeforeAll
    public static void cleanUpFolders() throws IOException {
        Path dataPath = Paths.get(DatabaseConstants.DATABASE_DATA_FOLDER + "/" + TEST_TABLE + DatabaseConstants.DATA_POSTFIX);
        Path indexPath = Paths.get(DatabaseConstants.DATABASE_DATA_FOLDER + "/" + TEST_TABLE + DatabaseConstants.INDEX_POST_FIX);
        Path metadataPath = Paths.get(DatabaseConstants.DATABASE_DATA_FOLDER + "/" + TEST_TABLE + DatabaseConstants.METADATA_POST_FIX);
        Files.deleteIfExists(dataPath);
        Files.deleteIfExists(indexPath);
        Files.deleteIfExists(metadataPath);
    }

}