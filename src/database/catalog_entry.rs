use std::path::PathBuf;

use rusqlite::Row;

use super::common::sha256_digest;

#[derive(PartialEq, Debug)]
pub(crate) struct CatalogEntry {
    pub(super) sha256: String,
    pub(super) path: String,
}

impl CatalogEntry {
    pub(crate) fn new(sha256: String, path: String) -> Self {
        Self { sha256, path }
    }

    pub(crate) fn sha256(&self) -> &str {
        &self.sha256
    }

    pub(crate) fn path(&self) -> PathBuf {
        PathBuf::from(&self.path)
    }
}

impl TryFrom<&PathBuf> for CatalogEntry {
    type Error = eyre::Report;

    fn try_from(path_buf: &PathBuf) -> std::prelude::v1::Result<Self, Self::Error> {
        let sha256 = sha256_digest(&path_buf)?;
        let path = path_buf.canonicalize()?;
        Ok(Self::new(sha256, path.to_string_lossy().to_string()))
    }
}

impl TryFrom<&Row<'_>> for CatalogEntry {
    type Error = rusqlite::Error;

    fn try_from(row: &Row<'_>) -> std::prelude::v1::Result<Self, Self::Error> {
        Ok(CatalogEntry {
            sha256: row.get(0)?,
            path: row.get(1)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::database::{catalog_entry::CatalogEntry, common::sha256_digest};

    #[test]
    fn try_from_creates_catalog_entry_from_path() {
        let path: PathBuf = ["Cargo.toml"].iter().collect();
        let CatalogEntry { sha256, path } = CatalogEntry::try_from(&path).unwrap();
        assert_eq!(sha256, sha256_digest(&PathBuf::from(path)).unwrap());
    }

    #[test]
    fn try_from_fails_to_create_catalog_entry_from_path() {
        let path: PathBuf = ["/tmp"].iter().collect();
        assert!(CatalogEntry::try_from(&path).is_err());
    }
}
