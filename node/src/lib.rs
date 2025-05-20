use cdb64::{CdbHash, Error as CdbError};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::fs::File;

#[napi(object)]
pub struct CdbEntry {
  pub key: Buffer,
  pub value: Buffer,
}

#[napi]
pub struct CdbWriter {
  inner: cdb64::CdbWriter<File, CdbHash>,
}

#[napi]
impl CdbWriter {
  #[napi(constructor)]
  pub fn new(path: String) -> napi::Result<Self> {
    let file = File::create(&path).map_err(|e| js_err(e.into()))?;
    let writer = cdb64::CdbWriter::<_, CdbHash>::new(file).map_err(js_err)?;
    Ok(CdbWriter { inner: writer })
  }

  #[napi]
  pub fn put(&mut self, key: Buffer, value: Buffer) -> napi::Result<()> {
    self.inner.put(&key, &value).map_err(js_err)?;
    Ok(())
  }

  #[napi]
  pub fn finalize(&mut self) -> napi::Result<()> {
    self.inner.finalize().map_err(js_err)?;
    Ok(())
  }
}

#[napi]
pub struct Cdb {
  inner: cdb64::Cdb<File, CdbHash>,
}

#[napi]
impl Cdb {
  #[napi(factory)]
  pub fn open(path: String) -> napi::Result<Self> {
    let cdb = cdb64::Cdb::<_, CdbHash>::open(&path).map_err(|e| js_err(e.into()))?;
    Ok(Cdb { inner: cdb })
  }

  #[napi]
  pub fn get(&self, key: Buffer) -> napi::Result<Option<Buffer>> {
    let v = self.inner.get(&key).map_err(|e| js_err(e.into()))?;
    Ok(v.map(Buffer::from))
  }

  #[napi]
  pub fn iter(&self) -> napi::Result<Vec<CdbEntry>> {
    let mut out = Vec::new();
    for entry in self.inner.iter() {
      let (k, v) = entry.map_err(|e| js_err(e.into()))?;
      out.push(CdbEntry {
        key: Buffer::from(k),
        value: Buffer::from(v),
      });
    }
    Ok(out)
  }
}

fn js_err(e: CdbError) -> napi::Error {
  napi::Error::from_reason(format!("{:?}", e))
}
