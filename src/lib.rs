use pyo3::prelude::*;

mod dataframe;
mod io;

use dataframe::DataFrame;

#[pyfunction]
fn read_csv(path: String) -> PyResult<DataFrame> {
    io::read_csv(&path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn read_parquet(path: String) -> PyResult<DataFrame> {
    io::read_parquet(&path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn read_json(path: String) -> PyResult<DataFrame> {
    io::read_json(&path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn read_excel(path: String) -> PyResult<DataFrame> {
    io::read_excel(&path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn read_sas(path: String) -> PyResult<DataFrame> {
    io::read_sas(&path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pymodule]
fn grizzly(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DataFrame>()?;
    m.add_function(wrap_pyfunction!(read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(read_json, m)?)?;
    m.add_function(wrap_pyfunction!(read_excel, m)?)?;
    m.add_function(wrap_pyfunction!(read_sas, m)?)?;
    Ok(())
}
