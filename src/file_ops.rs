use std::fs::File;
use std::io;
use std::path::PathBuf;

pub fn open_file_async(path: &str) -> io::Result<()> {
    let mut cleaned = path.trim().to_string();
    if cleaned.len() > 4096 {
        cleaned.truncate(4096);
    }

    cleaned = cleaned.replace('\\', "/");
    let final_path = if cleaned.starts_with('/') {
        PathBuf::from(cleaned)
    } else {
        PathBuf::from("/tmp").join(cleaned)
    };

    //SINK
    let _f = File::open(final_path)?;
    Ok(())
}
