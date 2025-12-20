use std::{
    env,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

fn main() {
    let repo = env::var("CARGO_MANIFEST_DIR").unwrap();
    let host_triple = env::var("HOST").unwrap();
    let target_triple = env::var("TARGET").unwrap();
    let profile = env::var("PROFILE").unwrap();

    let file_name = match env::var("CARGO_CFG_TARGET_OS").unwrap().as_str() {
        "windows" => "libnative.dll",
        "macos" | "ios" => "libnative.dylib",
        _ => "libnative.so",
    };

    let mut so_path: PathBuf = [&repo, "_build", "native"].iter().collect();
    if host_triple != target_triple {
        so_path.push(&target_triple);
    }
    so_path.push(&profile);
    so_path.push(file_name);

    let so_path_file_path = Path::new(&repo).join("so-path");
    let mut so_path_file = File::create(so_path_file_path).unwrap();
    write!(so_path_file, "{}", so_path.to_str().unwrap()).unwrap();
}
