use pyo3::prelude::*;
use arrow::record_batch::RecordBatch;
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use arrow_select::take::take;
use arrow_ord::sort::sort_to_indices;
use arrow_array::{StringArray, BooleanArray, UInt64Array, Float64Array, Array};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;

#[pyclass]
#[derive(Clone)]
pub struct DataFrame {
    pub batches: Vec<RecordBatch>,
}

#[pymethods]
impl DataFrame {
    #[new]
    pub fn new() -> Self {
        DataFrame { batches: Vec::new() }
    }

    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    pub fn column_count(&self) -> usize {
        self.batches.first().map(|b| b.num_columns()).unwrap_or(0)
    }

    pub fn __repr__(&self) -> String {
        format!("<grizzly.DataFrame ({} rows, {} columns)>", self.row_count(), self.column_count())
    }

    #[allow(non_snake_case)]
    #[pyo3(signature = (n=None))]
    pub fn head(&self, n: Option<usize>) -> DataFrame {
        let n = n.unwrap_or(5);
        let mut count = 0;
        let mut new_batches = Vec::new();

        for batch in &self.batches {
            if count >= n {
                break;
            }
            let take_n = (n - count).min(batch.num_rows());
            new_batches.push(batch.slice(0, take_n));
            count += take_n;
        }

        DataFrame { batches: new_batches }
    }

    pub fn to_csv(&self, path: &str) -> PyResult<()> {
        crate::io::to_csv(self, path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
    }

    pub fn to_parquet(&self, path: &str) -> PyResult<()> {
        crate::io::to_parquet(self, path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
    }

    pub fn to_json(&self, path: &str) -> PyResult<()> {
        crate::io::to_json(self, path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
    }

    pub fn to_excel(&self, path: &str) -> PyResult<()> {
        crate::io::to_excel(self, path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
    }

    pub fn filter_eq(&self, col_name: &str, value: &str) -> PyResult<DataFrame> {
        let mut filtered_batches = Vec::new();

        for batch in &self.batches {
            let col_idx = batch.schema().index_of(col_name)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
            
            let col = batch.column(col_idx);
            let string_col = cast(col, &DataType::Utf8)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("{}", e)))?;
            let string_array = string_col.as_any().downcast_ref::<StringArray>().unwrap();
            
            let mask: BooleanArray = BooleanArray::from_unary(string_array, |s| s == value);
            
            let filtered_batch = filter_record_batch(batch, &mask)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
            
            if filtered_batch.num_rows() > 0 {
                filtered_batches.push(filtered_batch);
            }
        }

        Ok(DataFrame { batches: filtered_batches })
    }

    #[pyo3(signature = (col_name, ascending=None))]
    pub fn sort(&self, col_name: &str, ascending: Option<bool>) -> PyResult<DataFrame> {
        if self.batches.is_empty() {
            return Ok(self.clone());
        }

        let schema = self.batches[0].schema();
        let combined_batch = concat_batches(&schema, &self.batches)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

        let col_idx = schema.index_of(col_name)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        
        let col = combined_batch.column(col_idx);
        let options = arrow_ord::sort::SortOptions {
            descending: !ascending.unwrap_or(true),
            nulls_first: false,
        };

        let indices = sort_to_indices(col, Some(options), None)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

        let mut sorted_columns = Vec::new();
        for i in 0..combined_batch.num_columns() {
            let sorted_col = take(combined_batch.column(i), &indices, None)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
            sorted_columns.push(sorted_col);
        }

        let sorted_batch = RecordBatch::try_new(schema, sorted_columns)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

        Ok(DataFrame { batches: vec![sorted_batch] })
    }

    pub fn concat(&self, other: &DataFrame) -> PyResult<DataFrame> {
        let mut new_batches = self.batches.clone();
        new_batches.extend(other.batches.clone());
        Ok(DataFrame { batches: new_batches })
    }

    pub fn groupby_sum(&self, group_col: &str, agg_col: &str) -> PyResult<DataFrame> {
        if self.batches.is_empty() { return Ok(self.clone()); }

        let mut groups: HashMap<String, f64> = HashMap::new();
        for batch in &self.batches {
            let g_idx = batch.schema().index_of(group_col).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
            let a_idx = batch.schema().index_of(agg_col).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
            
            let g_col = cast(batch.column(g_idx), &DataType::Utf8).unwrap();
            let a_col = cast(batch.column(a_idx), &DataType::Float64).unwrap();
            
            let g_arr = g_col.as_any().downcast_ref::<StringArray>().unwrap();
            let a_arr = a_col.as_any().downcast_ref::<Float64Array>().unwrap();
            
            for i in 0..batch.num_rows() {
                if !g_arr.is_null(i) && !a_arr.is_null(i) {
                    let key = g_arr.value(i).to_string();
                    let val = a_arr.value(i);
                    *groups.entry(key).or_insert(0.0) += val;
                }
            }
        }

        let mut group_keys = Vec::new();
        let mut agg_values = Vec::new();
        for (k, v) in groups {
            group_keys.push(k);
            agg_values.push(v);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new(group_col.to_string(), DataType::Utf8, false),
            Field::new(format!("{}_sum", agg_col), DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(group_keys)),
            Arc::new(Float64Array::from(agg_values)),
        ]).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

        Ok(DataFrame { batches: vec![batch] })
    }

    #[pyo3(signature = (other, on, how=None))]
    pub fn join(&self, other: &DataFrame, on: &str, how: Option<&str>) -> PyResult<DataFrame> {
        if self.batches.is_empty() || other.batches.is_empty() { return Ok(self.clone()); }
        
        let schema_l = self.batches[0].schema();
        let batch_l = concat_batches(&schema_l, &self.batches).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
        
        let schema_r = other.batches[0].schema();
        let batch_r = concat_batches(&schema_r, &other.batches).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
        
        let idx_l = schema_l.index_of(on).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        let idx_r = schema_r.index_of(on).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        
        let arr_l = cast(batch_l.column(idx_l), &DataType::Utf8).unwrap();
        let arr_l_str = arr_l.as_any().downcast_ref::<StringArray>().unwrap();
        
        let arr_r = cast(batch_r.column(idx_r), &DataType::Utf8).unwrap();
        let arr_r_str = arr_r.as_any().downcast_ref::<StringArray>().unwrap();
        
        let mut map_r = HashMap::new();
        for i in 0..batch_r.num_rows() {
            if !arr_r_str.is_null(i) {
                map_r.entry(arr_r_str.value(i).to_string()).or_insert_with(Vec::new).push(i);
            }
        }
        
        let mut indices_l: Vec<u64> = Vec::new();
        let mut indices_r: Vec<u64> = Vec::new();
        
        for i in 0..batch_l.num_rows() {
            if !arr_l_str.is_null(i) {
                let key = arr_l_str.value(i);
                if let Some(r_idxs) = map_r.get(key) {
                    for &r_idx in r_idxs {
                        indices_l.push(i as u64);
                        indices_r.push(r_idx as u64);
                    }
                }
            }
        }
        
        let idx_l_arr = UInt64Array::from(indices_l);
        let idx_r_arr = UInt64Array::from(indices_r);
        
        let mut joined_columns = Vec::new();
        let mut joined_fields = Vec::new();
        
        for i in 0..batch_l.num_columns() {
            let col = take(batch_l.column(i), &idx_l_arr, None).unwrap();
            joined_columns.push(col);
            joined_fields.push(schema_l.field(i).clone());
        }
        
        for i in 0..batch_r.num_columns() {
            if i == idx_r { continue; }
            let col = take(batch_r.column(i), &idx_r_arr, None).unwrap();
            joined_columns.push(col);
            let mut field = schema_r.field(i).clone();
            field = Field::new(format!("{}_right", field.name()), field.data_type().clone(), field.is_nullable());
            joined_fields.push(field);
        }
        
        let joined_schema = Arc::new(Schema::new(joined_fields));
        let joined_batch = RecordBatch::try_new(joined_schema, joined_columns).unwrap();
        
        Ok(DataFrame { batches: vec![joined_batch] })
    }

    pub fn show(&self) -> PyResult<()> {
        arrow::util::pretty::print_batches(&self.batches)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))
    }
}
