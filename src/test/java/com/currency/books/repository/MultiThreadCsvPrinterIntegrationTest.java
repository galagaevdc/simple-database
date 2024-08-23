package com.currency.books.repository;

import com.currency.books.utils.MultiThreadCsvPrinter;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@Slf4j
public class MultiThreadCsvPrinterIntegrationTest {

    private static final String TEST_DATA_CSV = "test-data.csv";
    private static final CountDownLatch waitFirstLineInserted = new CountDownLatch(1);
    private static final CountDownLatch waitSecondLineInserted = new CountDownLatch(1);
    private static final String[] FIRST_LINE = {"1", "1"};
    private static final String[] SECOND_LINE = {"2", "2"};
    private static final String[] THIRD_LINE = {"3", "3"};

    @Test
    public void testConcurrentWrites() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        Path path = Paths.get(TEST_DATA_CSV);
        final MultiThreadCsvPrinter dataPrinter = new MultiThreadCsvPrinter(
                path, 0);
        try {

            ExecutorService executorService = Executors.newFixedThreadPool(10);

            final List<Future<?>> futures = new ArrayList<>();
            futures.add(executorService.submit(() -> writeFirst(dataPrinter)));
            futures.add(executorService.submit(() -> writeSecond(dataPrinter)));

            for (Future<?> future : futures) {
                future.get(1, TimeUnit.SECONDS);
            }

            dataPrinter.flush();
            dataPrinter.close();

            var line1 = readLine(0);
            var line2 = readLine(1);
            var line3 = readLine(2);

            assertArrayEquals(line1, FIRST_LINE);
            assertArrayEquals(line2, SECOND_LINE);
            assertArrayEquals(line3, THIRD_LINE);

        } finally {
            if (Files.exists(path)) {
                Files.delete(path);
            }
        }

    }

    private static String[] readLine(final int rowIndex) throws IOException {
        final CSVReader reader = new CSVReader(new FileReader(TEST_DATA_CSV),
                CSVParser.DEFAULT_SEPARATOR, CSVParser.DEFAULT_QUOTE_CHARACTER, rowIndex);
        final var line = reader.readNext();
        reader.close();
        return line;
    }

    private void writeFirst(final MultiThreadCsvPrinter dataPrinter) {
        log.info("Inserting first row");
        try {
            dataPrinter.writeLine(FIRST_LINE);
            waitFirstLineInserted.countDown();
            waitSecondLineInserted.await();
            log.info("Inserting third row");
            dataPrinter.writeLine(THIRD_LINE);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeSecond(final MultiThreadCsvPrinter dataPrinter) {
        try {
            waitFirstLineInserted.await();
            log.info("Inserting second row");
            dataPrinter.writeLine(SECOND_LINE);
            waitSecondLineInserted.countDown();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
