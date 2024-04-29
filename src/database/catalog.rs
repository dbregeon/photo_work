use std::path::PathBuf;

use eyre::{eyre, Context, Result};
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

pub(crate) fn persist_catalog_entries(
    connection: &mut Connection,
    entries: &Vec<CatalogEntry>,
) -> Result<usize> {
    let mut tx = connection.transaction()?;
    match catalog_insert_all(&mut tx, entries) {
        Ok(count) => {
            tx.commit()?;
            Ok(count)
        }
        e => e,
    }
}

fn catalog_insert_all(transaction: &mut Transaction, entries: &Vec<CatalogEntry>) -> Result<usize> {
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

pub(crate) fn find_duplicates(connection: &Connection) -> Result<Vec<CatalogEntry>> {
    let mut statement = connection.prepare("SELECT catalog.hash, catalog.path FROM catalog GROUP BY catalog.hash HAVING COUNT(catalog.path) > 1 ORDER BY catalog.hash")?;
    let results = statement
        .query_map([], |row| {
            Ok(CatalogEntry {
                sha256: row.get(0)?,
                path: row.get(1)?,
            })
        })?
        .collect::<Vec<Result<CatalogEntry, rusqlite::Error>>>();
    Ok(results
        .into_iter()
        .collect::<Result<Vec<CatalogEntry>, rusqlite::Error>>()?)
}

pub(crate) fn foreach_entry<F>(connection: &Connection, mut f: F) -> Result<usize>
where
    F: FnMut(CatalogEntry) -> Result<()>,
{
    let mut query = connection.prepare("SELECT hash, path FROM catalog")?;
    let entries = query.query_map([], |r| {
        Ok(CatalogEntry::new(
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?.into(),
        ))
    })?;
    let mut count = 0;
    let mut errors = vec![];
    for entry_mapping_result in entries {
        match entry_mapping_result {
            Ok(entry) => match f(entry) {
                Ok(()) => count += 1,
                Err(e) => errors.push(e.to_string()),
            },
            Err(e) => errors.push(e.to_string()),
        }
    }
    if errors.len() == 0 {
        Ok(count)
    } else {
        Err(eyre!(errors.join("\n")))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use eyre::eyre;
    use rusqlite::{params, Connection};

    use crate::database::{
        catalog::{catalog_insert_all, foreach_entry},
        common::sha256_digest,
        library::persist_library_entries,
        library_entry::LibraryEntry,
        migrate,
    };

    use super::{persist_catalog_entries, select_from_catalog, CatalogEntry};

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

    fn new_database() -> Connection {
        let mut connection = new_connection();
        migrate(&mut connection).unwrap();
        connection
    }

    fn new_connection() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    fn new_database_containing(entries: &Vec<CatalogEntry>) -> Connection {
        let mut connection = new_database();
        let _count = persist_catalog_entries(&mut connection, entries);
        connection
    }

    #[test]
    fn catalog_insert_all_returns_count_of_insertions() {
        let mut connection = new_database();
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
        let mut connection = new_database();

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
        let mut connection = new_database();
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
    fn persist_catalog_entries_rollbacks_on_error() {
        let mut connection = new_database();
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
        let result = persist_catalog_entries(&mut connection, &entries);
        assert!(catalog_contains(
            &mut connection,
            &CatalogEntry {
                sha256: "1".to_string(),
                path: "a".to_string(),
            }
        ));
        assert!(!catalog_contains(
            &mut connection,
            &CatalogEntry {
                sha256: "2".to_string(),
                path: "b".to_string(),
            }
        ));
        assert_eq!(
            result.err().unwrap().to_string(),
            "UNIQUE constraint failed: catalog.path".to_string()
        );
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
        let mut connection = new_database();
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
        persist_catalog_entries(&mut connection, &entries).unwrap();
        let results = select_from_catalog(&connection, "a").unwrap();
        assert_eq!(entries, results);
    }

    #[test]
    fn select_from_catalog_does_not_return_entries_in_library() {
        let mut connection = new_database();
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
        persist_catalog_entries(&mut connection, &entries).unwrap();
        persist_library_entries(
            &mut connection,
            &vec![LibraryEntry::new("1".to_string(), PathBuf::from("a/aa"))],
        )
        .unwrap();
        let results = select_from_catalog(&connection, "a").unwrap();
        assert_eq!(expected_results, results);
    }

    #[test]
    fn select_from_catalog_does_not_return_duplicate_hash_entries() {
        let mut connection = new_database();
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
        persist_catalog_entries(&mut connection, &entries).unwrap();
        let results = select_from_catalog(&connection, "a").unwrap();
        assert_eq!(1, results.len());
    }

    #[test]
    fn foreach_entry_applies_the_function_to_each_entry() {
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
        let mut connection = new_database_containing(&entries);
        let mut entry_hashes = vec![];
        let iterated_count = foreach_entry(&mut connection, |e| {
            entry_hashes.push(e.sha256().to_owned());
            Ok(())
        })
        .unwrap();
        assert_eq!(entries.len(), iterated_count);
        assert_eq!(vec!("1", "2"), entry_hashes);
    }

    #[test]
    fn foreach_entry_returns_error_when_query_fails() {
        let connection = new_connection();
        assert_eq!(
            "no such table: catalog",
            foreach_entry(&connection, |_e| Ok(()))
                .err()
                .unwrap()
                .to_string()
        )
    }

    #[test]
    fn foreach_entry_returns_error_when_parameter_function_does() {
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
        let connection = new_database_containing(&entries);
        assert_eq!(
            "invalid entry",
            foreach_entry(&connection, |e| if e.sha256() == "1" {
                Ok(())
            } else {
                Err(eyre!("invalid entry"))
            })
            .err()
            .unwrap()
            .to_string()
        );
    }

    #[test]
    fn foreach_entry_returns_error_when_row_cannot_be_converted_to_entry() {
        let connection = new_connection();
        connection
            .execute("create table catalog (hash integer, path string)", [])
            .unwrap();
        connection
            .execute(
                "INSERT INTO catalog (hash, path) values (?1, ?2)",
                params!(1, "test value"),
            )
            .unwrap();

        assert_eq!(
            "Invalid column type Integer at index: 0, name: hash",
            foreach_entry(&connection, |_e| Ok(()))
                .err()
                .unwrap()
                .to_string()
        )
    }
}
