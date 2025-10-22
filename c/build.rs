extern crate cbindgen;

use std::{env, path::PathBuf};

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let out_dir = PathBuf::from(&crate_dir).join("include");

    // Ensure the output directory exists
    if !out_dir.exists() {
        std::fs::create_dir_all(&out_dir).expect("Failed to create include directory");
    }

    let header_path = out_dir.join("cdb64.h");

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(
            cbindgen::Config::from_file("cbindgen.toml").expect("Failed to load cbindgen.toml"),
        )
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(header_path);
}
