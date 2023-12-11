use std::path::PathBuf;

use eyre::{Context, Result};
use rusqlite::{params, Connection, Transaction};

use super::common::sha256_digest;

#[derive(PartialEq, Debug)]
pub(crate) struct CatalogEntry {
    sha256: String,
    path: String,
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

pub(crate) fn catalog_insert_all(
    transaction: &mut Transaction,
    entries: &Vec<CatalogEntry>,
) -> Result<usize> {
    let mut count = 0;
    let mut statement = transaction.prepare("INSERT INTO catalog (hash, path) values (?1, ?2)")?;
    for CatalogEntry { sha256, path } in entries {
        count += statement.execute([sha256, path])?;
    }
    Ok(count)
}

pub(crate) fn select_from_catalog(
    connection: &Connection,
    path_prefix: &str,
) -> Result<Vec<CatalogEntry>> {
    let mut statement = connection.prepare("SELECT catalog.hash, catalog.path FROM catalog LEFT JOIN library ON catalog.hash = library.hash WHERE catalog.path like ?1 AND library.hash IS NULL GROUP BY catalog.hash")?;
    let results = statement
        .query_map(params!([path_prefix, "%"].join("")), |row| {
            Ok(CatalogEntry {
                sha256: row.get(0)?,
                path: row.get(1)?,
            })
        })?
        .collect::<Vec<Result<CatalogEntry, rusqlite::Error>>>();
    results
        .into_iter()
        .collect::<Result<Vec<CatalogEntry>, rusqlite::Error>>()
        .wrap_err_with(|| format!("Failed to read catalog for path {}", path_prefix))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rusqlite::{params, Connection};

    use crate::database::{
        catalog::catalog_insert_all, common::sha256_digest, library_entry::LibraryEntry, migrate,
    };

    use super::{select_from_catalog, CatalogEntry};

    fn catalog_contains(connection: &mut Connection, entry: &CatalogEntry) -> bool {
        match connection.query_row(
            "SELECT true FROM catalog WHERE hash = ?1 AND path = ?2",
            params!(entry.sha256, entry.path),
            |row| row.get::<_, bool>(0),
        ) {
            Ok(b) => b,
            Err(_) => false,
        }
    }

    fn connection() -> Connection {
        let mut connection = Connection::open_in_memory().unwrap();
        migrate(&mut connection).unwrap();
        connection
    }

    fn insert_catalog_entries(connection: &mut Connection, entries: &Vec<CatalogEntry>) {
        for entry in entries {
            insert_catalog_entry(connection, entry);
        }
    }

    fn insert_catalog_entry(connection: &mut Connection, entry: &CatalogEntry) {
        connection
            .execute(
                "INSERT INTO catalog (hash, path) values (?1, ?2)",
                params!(entry.sha256, entry.path),
            )
            .unwrap();
    }

    fn insert_library_entry(connection: &mut Connection, entry: &LibraryEntry) {
        connection
            .execute(
                "INSERT INTO library (hash, path) values (?1, ?2)",
                params!(entry.sha256(), entry.path().to_string_lossy().to_string()),
            )
            .unwrap();
    }

    #[test]
    fn catalog_insert_all_returns_count_of_insertions() {
        let mut connection = connection();
        let mut transaction = connection.transaction().unwrap();
        let entries = vec![
            CatalogEntry {
                sha256: "1".to_string(),
                path: "a".to_string(),
            },
            CatalogEntry {
                sha256: "2".to_string(),
                path: "b".to_string(),
            },
        ];
        assert_eq!(
            entries.len(),
            catalog_insert_all(&mut transaction, &entries).unwrap()
        );
    }

    #[test]
    fn catalog_insert_all_inserts_into_the_catalog_table() {
        let mut connection = connection();

        let mut transaction = connection.transaction().unwrap();
        let entries = vec![
            CatalogEntry {
                sha256: "1".to_string(),
                path: "a".to_string(),
            },
            CatalogEntry {
                sha256: "2".to_string(),
                path: "b".to_string(),
            },
        ];
        catalog_insert_all(&mut transaction, &entries).unwrap();
        transaction.commit().unwrap();

        assert!(catalog_contains(&mut connection, &entries[0]));
        assert!(catalog_contains(&mut connection, &entries[1]));
    }

    #[test]
    fn catalog_insert_all_results_in_error_when_one_is_duplicate() {
        let mut connection = connection();
        let entries = vec![
            CatalogEntry {
                sha256: "1".to_string(),
                path: "a".to_string(),
            },
            CatalogEntry {
                sha256: "2".to_string(),
                path: "b".to_string(),
            },
        ];
        connection
            .execute(
                "INSERT INTO catalog (hash, path) values (?1, ?2)",
                params!(&entries[0].sha256, &entries[0].path),
            )
            .unwrap();
        let mut transaction = connection.transaction().unwrap();
        assert!(catalog_insert_all(&mut transaction, &entries).is_err());
        transaction.rollback().unwrap();
        assert!(!catalog_contains(&mut connection, &entries[1]));
    }

    #[test]
    fn try_from_creates_catalog_entry_from_path() {
        let path: PathBuf = ["Cargo.toml"].iter().collect();
        let CatalogEntry { sha256, path } = CatalogEntry::try_from(&path).unwrap();
        assert_eq!(sha256, sha256_digest(&PathBuf::from(path)).unwrap());
    }

    #[test]
    fn try_from_fails_to_create_catalog_entry_from_path() {
        let path = ["/tmp"].iter().collect();
        assert!(CatalogEntry::try_from(&path).is_err());
    }

    #[test]
    fn select_from_catalog_returns_the_catalog_entries() {
        let mut connection = connection();
        let entries = vec![
            CatalogEntry {
                sha256: "1".to_string(),
                path: "a/a".to_string(),
            },
            CatalogEntry {
                sha256: "2".to_string(),
                path: "a/b".to_string(),
            },
        ];
        insert_catalog_entries(&mut connection, &entries);
        let results = select_from_catalog(&connection, "a").unwrap();
        assert_eq!(entries, results);
    }

    #[test]
    fn select_from_catalog_does_not_return_entries_in_library() {
        let mut connection = connection();
        let entries = vec![
            CatalogEntry {
                sha256: "1".to_string(),
                path: "a/a".to_string(),
            },
            CatalogEntry {
                sha256: "2".to_string(),
                path: "a/b".to_string(),
            },
        ];
        let expected_results = vec![CatalogEntry {
            sha256: "2".to_string(),
            path: "a/b".to_string(),
        }];
        insert_catalog_entries(&mut connection, &entries);
        insert_library_entry(
            &mut connection,
            &LibraryEntry::new("1".to_string(), PathBuf::from("a/aa")),
        );
        let results = select_from_catalog(&connection, "a").unwrap();
        assert_eq!(expected_results, results);
    }

    #[test]
    fn select_from_catalog_does_not_return_duplicate_hash_entries() {
        let mut connection = connection();
        let entries = vec![
            CatalogEntry {
                sha256: "1".to_string(),
                path: "a/a".to_string(),
            },
            CatalogEntry {
                sha256: "1".to_string(),
                path: "a/b".to_string(),
            },
        ];
        insert_catalog_entries(&mut connection, &entries);
        let results = select_from_catalog(&connection, "a").unwrap();
        assert_eq!(1, results.len());
    }
}
