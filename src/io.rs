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

use rayon::prelude::*;
use memmap2::Mmap;
pub fn read_csv(path: &str) -> Result<DataFrame> {
    let file = File::open(path).with_context(|| format!("Failed to open CSV file: {}", path))?;
    let mmap = unsafe { Mmap::map(&file)? };
    let bytes = &mmap[..];

    // Faster schema inference by limiting to 1000 records
    let schema = infer_schema_from_files(&[path.to_string()], b',', Some(1000), true)
        .with_context(|| format!("Failed to infer schema for CSV file: {}", path))?;
    let schema_arc = Arc::new(schema);

    // Determine parallel segments
    let n_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    
    if n_threads <= 1 || bytes.len() < 1024 * 1024 {
        // Fallback for single thread or small files
        let csv_reader = ReaderBuilder::new(schema_arc)
            .with_header(true)
            .build(bytes)?;
        let batches = csv_reader
            .collect::<std::result::Result<Vec<RecordBatch>, _>>()
            .context("Failed to read CSV batches")?;
        return Ok(DataFrame { batches });
    }

    let chunk_size = bytes.len() / n_threads;
    let mut offsets = Vec::new();
    offsets.push(0);

    let mut in_quotes = false;
    let mut last_pos = 0;
    
    // Scan for boundaries
    for i in 1..n_threads {
        let target = i * chunk_size;
        let mut pos = target;
        
        // We need to know the state (in_quotes) at the start of the chunk.
        // The most robust way is to scan from the last known boundary.
        let mut current_in_quotes = in_quotes;
        for j in last_pos..pos {
            if bytes[j] == b'"' {
                current_in_quotes = !current_in_quotes;
            }
        }
        in_quotes = current_in_quotes;
        
        // Now find the next newline that is NOT inside quotes
        while pos < bytes.len() {
            if bytes[pos] == b'"' {
                in_quotes = !in_quotes;
            } else if bytes[pos] == b'\n' && !in_quotes {
                pos += 1;
                break;
            }
            pos += 1;
        }
        
        if pos < bytes.len() {
            offsets.push(pos);
            last_pos = pos;
        }
    }
    offsets.push(bytes.len());
    offsets.dedup();

    // Process chunks in parallel
    let results: Result<Vec<Vec<RecordBatch>>> = offsets
        .windows(2)
        .enumerate()
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(|(id, window)| {
            let start = window[0];
            let end = window[1];
            if start >= end {
                return Ok(vec![]);
            }

            let chunk_bytes = &bytes[start..end];
            let mut builder = ReaderBuilder::new(schema_arc.clone())
                .with_batch_size(65536);
            
            // Only the first chunk should treat the first line as a header
            if id == 0 {
                builder = builder.with_header(true);
            } else {
                builder = builder.with_header(false);
            }

            let csv_reader = builder.build(chunk_bytes)?;
            let chunk_batches: Vec<RecordBatch> = csv_reader
                .collect::<std::result::Result<Vec<RecordBatch>, _>>()?;
            
            Ok(chunk_batches)
        })
        .collect();

    let batches = results?
        .into_iter()
        .flatten()
        .collect();

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
