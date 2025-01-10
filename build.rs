use std::path::Path;

fn main() {
    // Tell cargo to rerun if the schema file changes
    println!("cargo:rerun-if-changed=src/embedding.fbs");
    
    // Run flatc compiler
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("flatbuffer/embedding.fbs")],
        out_dir: Path::new("src"),
        ..Default::default()
    }).expect("flatc failed to run");
} 