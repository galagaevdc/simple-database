package com.currency.books.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class Row {
    private List<ColumnData> columns;
}
