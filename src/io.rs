use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use arrow_csv::{ReaderBuilder, Writer, reader::infer_schema_from_files};
use arrow::record_batch::RecordBatch;
use anyhow::{Result, Context};
use crate::dataframe::DataFrame;
use std::sync::Arc;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use arrow_json::ReaderBuilder as JsonReaderBuilder;
use arrow_json::LineDelimitedWriter;
use arrow_json::reader::infer_json_schema;
use calamine::{Reader, Xlsx, open_workbook};
use rust_xlsxwriter::Workbook;
use arrow_array::{StringArray, Array};
use arrow_schema::{Field, Schema, DataType};

pub fn read_csv(path: &str) -> Result<DataFrame> {
    let schema = infer_schema_from_files(&[path.to_string()], b',', None, true)
        .with_context(|| format!("Failed to infer schema for CSV file: {}", path))?;
    let file = File::open(path).with_context(|| format!("Failed to open CSV file: {}", path))?;
    let reader = BufReader::new(file);
    let csv_reader = ReaderBuilder::new(Arc::new(schema))
        .with_header(true)
        .build(reader)?;
    let batches = csv_reader
        .collect::<std::result::Result<Vec<RecordBatch>, _>>()
        .context("Failed to read CSV batches")?;
    Ok(DataFrame { batches })
}

pub fn to_csv(df: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path).with_context(|| format!("Failed to create CSV file: {}", path))?;
    let mut writer = Writer::new(file);
    for batch in &df.batches {
        writer.write(batch)?;
    }
    Ok(())
}

pub fn read_parquet(path: &str) -> Result<DataFrame> {
    let file = File::open(path).with_context(|| format!("Failed to open Parquet file: {}", path))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let batches = reader
        .collect::<std::result::Result<Vec<RecordBatch>, _>>()
        .context("Failed to read Parquet batches")?;
    Ok(DataFrame { batches })
}

pub fn to_parquet(df: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path).with_context(|| format!("Failed to create Parquet file: {}", path))?;
    let batches = &df.batches;
    if batches.is_empty() {
        return Err(anyhow::anyhow!("DataFrame is empty"));
    }
    let schema = batches[0].schema();
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

pub fn read_json(path: &str) -> Result<DataFrame> {
    let file = File::open(path).with_context(|| format!("Failed to open JSON file: {}", path))?;
    let mut reader = BufReader::new(file);
    let (schema, _) = infer_json_schema(&mut reader, None)?;
    reader.seek(SeekFrom::Start(0))?;
    let json_reader = JsonReaderBuilder::new(Arc::new(schema)).build(reader)?;
    let batches = json_reader
        .collect::<std::result::Result<Vec<RecordBatch>, _>>()
        .context("Failed to read JSON batches")?;
    Ok(DataFrame { batches })
}

pub fn to_json(df: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path).with_context(|| format!("Failed to create JSON file: {}", path))?;
    let mut writer = LineDelimitedWriter::new(file);
    for batch in &df.batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(())
}

pub fn read_excel(path: &str) -> Result<DataFrame> {
    let mut workbook: Xlsx<_> = open_workbook(path).with_context(|| format!("Failed to open Excel file: {}", path))?;
    let sheet_name = workbook.sheet_names().get(0).cloned().context("No sheets in workbook")?;
    let range = workbook.worksheet_range(&sheet_name).context("Failed to get sheet range")?;
    
    let mut rows = range.rows();
    let header = rows.next().context("Sheet is empty")?;
    
    let mut fields = Vec::new();
    for cell in header {
        fields.push(Field::new(cell.to_string(), DataType::Utf8, true));
    }
    let schema = Arc::new(Schema::new(fields));
    
    let mut column_data: Vec<Vec<String>> = vec![Vec::new(); schema.fields().len()];

    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < column_data.len() {
                column_data[i].push(cell.to_string());
            }
        }
    }

    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
    for data in column_data {
        arrays.push(Arc::new(StringArray::from(data)));
    }

    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(DataFrame { batches: vec![batch] })
}

pub fn to_excel(df: &DataFrame, path: &str) -> Result<()> {
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    
    if df.batches.is_empty() {
        workbook.save(path)?;
        return Ok(());
    }

    let schema = df.batches[0].schema();
    // Write header
    for (i, field) in schema.fields().iter().enumerate() {
        worksheet.write_string(0, i as u16, field.name())?;
    }

    let mut row_idx = 1;
    for batch in &df.batches {
        for r in 0..batch.num_rows() {
            for c in 0..batch.num_columns() {
                let col = batch.column(c);
                let string_col = arrow_cast::cast(col, &DataType::Utf8)?;
                let string_array = string_col.as_any().downcast_ref::<StringArray>().unwrap();
                worksheet.write_string(row_idx as u32, c as u16, string_array.value(r))?;
            }
            row_idx += 1;
        }
    }

    workbook.save(path)?;
    Ok(())
}
