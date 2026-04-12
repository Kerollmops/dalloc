// build.rs
fn main() {
    let page_size = page_size::get();
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest_path = std::path::Path::new(&out_dir).join("page_size.rs");
    std::fs::write(&dest_path, format!("pub const PAGE_SIZE: usize = {page_size};")).unwrap();
    println!("cargo:rerun-if-changed=build.rs");
}
