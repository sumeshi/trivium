use std::{env, fs, path::PathBuf};

fn main() {
    let directory =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("../../definitions/formats");
    println!("cargo:rerun-if-changed={}", directory.display());
    let mut files = fs::read_dir(&directory)
        .expect("definitions/formats must be present")
        .map(|entry| {
            entry
                .expect("failed to read format definition entry")
                .path()
        })
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect::<Vec<_>>();
    files.sort();
    let mut source = String::from("const BUILTIN_FORMAT_JSON: &[(&str, &str)] = &[\n");
    for file in files {
        assert!(file.is_file(), "format definitions must be JSON files");
        let name = file.file_stem().unwrap().to_str().unwrap();
        let absolute = file
            .canonicalize()
            .expect("failed to locate format definition");
        source.push_str(&format!(
            "    ({name:?}, include_str!({:?})),\n",
            absolute
                .to_str()
                .expect("format definition path is not UTF-8")
        ));
    }
    source.push_str("];\n");
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("builtin_formats.rs"),
        source,
    )
    .expect("failed to embed format definitions");
}
