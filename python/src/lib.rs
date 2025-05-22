use cdb64::{CdbHash, Error as CdbError};
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use std::fs::File;

#[pyclass(name = "CdbWriter")]
struct PyCdbWriter {
    inner: cdb64::CdbWriter<File, CdbHash>,
}

#[pymethods]
impl PyCdbWriter {
    #[new]
    fn new(path: String) -> PyResult<Self> {
        let file = File::create(&path).map_err(|e| PyIOError::new_err(e.to_string()))?;
        let writer = cdb64::CdbWriter::<_, CdbHash>::new(file).map_err(map_cdb_err)?;
        Ok(PyCdbWriter { inner: writer })
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> PyResult<()> {
        self.inner.put(key, value).map_err(map_cdb_err)?;
        Ok(())
    }

    fn finalize(&mut self) -> PyResult<()> {
        self.inner.finalize().map_err(map_cdb_err)?;
        Ok(())
    }
}

#[pyclass(name = "Cdb")]
struct PyCdb {
    inner: cdb64::Cdb<File, CdbHash>,
}

#[pymethods]
impl PyCdb {
    #[staticmethod]
    fn open(path: String) -> PyResult<Self> {
        let cdb =
            cdb64::Cdb::<_, CdbHash>::open(&path).map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(PyCdb { inner: cdb })
    }

    fn get<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match self.inner.get(key) {
            Ok(Some(value)) => Ok(Some(PyBytes::new(py, &value))),
            Ok(None) => Ok(None),
            Err(e) => Err(map_cdb_err(e.into())),
        }
    }

    fn iter<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut list = Vec::new();
        for entry in self.inner.iter() {
            match entry {
                Ok((k, v)) => {
                    let py_key = PyBytes::new(py, &k);
                    let py_value = PyBytes::new(py, &v);
                    list.push((py_key, py_value));
                }
                Err(e) => return Err(map_cdb_err(e.into())),
            }
        }

        PyList::new(py, &list)
    }
}

fn map_cdb_err(e: CdbError) -> PyErr {
    PyIOError::new_err(e.to_string())
}

#[pymodule]
fn cdb64_python(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCdbWriter>()?;
    m.add_class::<PyCdb>()?;
    Ok(())
}
