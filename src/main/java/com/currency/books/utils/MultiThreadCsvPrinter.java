package com.currency.books.utils;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

public class MultiThreadCsvPrinter implements AutoCloseable {
    private final CSVWriter csvWriter;
    private Integer rowIndex;

    public MultiThreadCsvPrinter(final Path path, final Integer rowIndex) throws IOException {
        this.csvWriter = writerFromPath(path);
        this.rowIndex = rowIndex;
    }

    private CSVWriter writerFromPath(final Path path) throws IOException {
        final FileWriter fileWriter = new FileWriter(path.toFile());
        return new CSVWriter(fileWriter);
    }

    public synchronized Integer writeLine(final String... line) throws IOException {
        this.csvWriter.writeNext(line);
        return rowIndex++;
    }

    public synchronized void flush() throws IOException {
        this.csvWriter.flush();
    }

    public void close() throws IOException {
        this.csvWriter.close();
    }
}
