# Grizzly 🐻

Grizzly is a high-performance Python data manipulation library built from scratch using **Rust** and **Apache Arrow**. It provides a fast and memory-efficient alternative for common data processing tasks, with a focus on ease of use and interoperability with standard formats.

## 🚀 Features

-   **Multi-format I/O**:
    -   Read/Write **CSV** (with schema inference).
    -   Read/Write **Parquet** (native Arrow integration).
    -   Read/Write **JSON** (line-delimited formats).
    -   Read/Write **Excel** (powered by `calamine` and `rust_xlsxwriter`).
-   **Core Manipulation**:
    -   Fast filtering with `filter_eq`.
    -   Global sorting with `sort`.
    -   Seamless DataFrame concatenation with `concat`.
-   **Advanced Operations**:
    -   High-performance **Joins** (Inner joins on keys).
    -   Data aggregation with **Groupby** (`groupby_sum`).

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
