# Grizzly 🐻

Grizzly is a high-performance Python data manipulation library built from scratch using **Rust** and **Apache Arrow**. It provides a fast and memory-efficient alternative for common data processing tasks, with a focus on ease of use and interoperability with standard formats.

## 🚀 Features

-   **Multi-format I/O**:
    -   Read/Write **CSV** (with schema inference).
    -   Read/Write **Parquet** (native Arrow integration).
    -   Read/Write **JSON** (line-delimited formats).
    -   Read/Write **Excel** (powered by `calamine` and `rust_xlsxwriter`).
    -   Read **SAS7BDAT** (SAS binary files).
-   **Core Manipulation**:
    -   Fast filtering with `filter_eq`.
    -   Global sorting with `sort`.
    -   Seamless DataFrame concatenation with `concat`.
-   **Advanced Operations**:
    -   High-performance **Joins** (Inner joins on keys).
    -   Data aggregation with **Groupby** (`groupby_sum`).

## 📖 API Reference

### Module-level Functions

- `read_csv(path: str) -> DataFrame`: Reads a CSV file into a DataFrame.
- `read_parquet(path: str) -> DataFrame`: Reads a Parquet file into a DataFrame.
- `read_json(path: str) -> DataFrame`: Reads a line-delimited JSON file into a DataFrame.
- `read_excel(path: str) -> DataFrame`: Reads an Excel file (.xlsx) into a DataFrame.
- `read_sas(path: str) -> DataFrame`: Reads a SAS7BDAT file into a DataFrame.

### DataFrame Methods

- `row_count() -> int`: Returns the number of rows in the DataFrame.
- `column_count() -> int`: Returns the number of columns.
- `shape -> tuple[int, int]`: Returns the shape of the DataFrame as (rows, columns).
- `head(n: int = 5) -> DataFrame`: Returns a new DataFrame with the first `n` rows.
- `show(n: int = 10)`: Prints the first `n` rows of the DataFrame in a pretty table, including column data types.
- `query(sql: str) -> DataFrame`: Executes a SQL query on the DataFrame and returns the result.
- `filter_eq(col_name: str, value: str) -> DataFrame`: Filters rows where the specified column equals the given value.
- `sort(col_name: str, ascending: bool = True) -> DataFrame`: Sorts the DataFrame by the specified column.
- `concat(other: DataFrame) -> DataFrame`: Concatenates two DataFrames.
- `groupby_sum(group_col: str, agg_col: str) -> DataFrame`: Groups by `group_col` and sums the `agg_col`.
- `join(other: DataFrame, on: str, how: str = "inner") -> DataFrame`: Joins with another DataFrame on a common column. Supports `how="inner"` and `how="left"`.
- `to_csv(path: str)`: Exports the DataFrame to a CSV file.
- `to_parquet(path: str)`: Exports the DataFrame to a Parquet file.
- `to_json(path: str)`: Exports the DataFrame to a JSON file.
- `to_excel(path: str)`: Exports the DataFrame to an Excel file.

## 🛠 Installation

You can build and install grizzly locally using `maturin`:

```bash
maturin develop
```

## 📖 Quick Start

```python
import grizzly

# Load data from any source
df = grizzly.read_csv("data.csv")

# Perform manipulations
df_filtered = df.filter_eq("category", "electronics")
df_sorted = df_filtered.sort("price", ascending=True)

# Aggregate results
report = df_sorted.groupby_sum("brand", "sales")

# Export to your desired format
report.to_parquet("summary.parquet")
```

## 🧪 Testing

Run the comprehensive test suite using `pytest`:

```bash
pytest tests/
```

## 📜 License

MIT
