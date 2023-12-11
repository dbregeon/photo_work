use std::{
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

use sha2::{Digest, Sha256};

/// calculates sha256 digest as lowercase hex string
pub(crate) fn sha256_digest(path: &PathBuf) -> Result<String, std::io::Error> {
    let input = File::open(path)?;
    let mut reader = BufReader::new(input);

    let digest = {
        let mut hasher = Sha256::new();
        let mut buffer = [0; 1024];
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        hasher.finalize()
    };
    Ok(format!("{:X}", digest))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    #[test]
    #[cfg(target_os = "linux")]
    fn sha256_digest_is_the_same_as_the_system_sha256() {
        use std::process::Command;

        use crate::database::common::sha256_digest;

        let path: PathBuf = ["Cargo.toml"].iter().collect();
        let system_out = Command::new("sh")
            .arg("-c")
            .arg("sha256sum -b Cargo.toml | cut -d \" \" -f 1 ")
            .output()
            .unwrap()
            .stdout;
        let system_sha256 = std::str::from_utf8(&system_out)
            .unwrap()
            .trim_end()
            .to_uppercase();
        assert_eq!(system_sha256, sha256_digest(&path).unwrap());
    }
}
