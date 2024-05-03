use eyre::{eyre, Result};
use rusqlite::{params, Connection, Params, Statement, Transaction};

use super::catalog_entry::CatalogEntry;

pub(crate) fn persist_catalog_entries(
    connection: &mut Connection,
    entries: &Vec<CatalogEntry>,
) -> Result<usize> {
    let mut transaction = connection.transaction()?;
    let count = catalog_insert_all(&mut transaction, entries)?;
    assert!(count == entries.len());
    transaction.commit()?;
    Ok(count)
}

fn catalog_insert_all(transaction: &mut Transaction, entries: &Vec<CatalogEntry>) -> Result<usize> {
    let mut count = 0;
    let mut statement = transaction.prepare("INSERT INTO catalog (hash, path) values (?1, ?2)")?;
    for entry in entries {
        count += catalog_insert(&mut statement, entry)?;
    }
    Ok(count)
}

fn catalog_insert(
    statement: &mut Statement,
    CatalogEntry { sha256, path }: &CatalogEntry,
) -> Result<usize> {
    statement
        .execute([sha256, path])
        .map_err(|e| eyre!("Failed to insert ({}, {}): {}", sha256, path, e))
}

pub(crate) fn select_from_catalog(
    connection: &Connection,
    path_prefix: &str,
) -> Result<Vec<CatalogEntry>> {
    let mut statement = connection.prepare("SELECT catalog.hash, catalog.path FROM catalog LEFT JOIN library ON catalog.hash = library.hash WHERE catalog.path like ?1 AND library.hash IS NULL GROUP BY catalog.hash")?;
    query(&mut statement, params!([path_prefix, "%"].join("")))
}

pub(crate) fn find_duplicates(connection: &Connection) -> Result<Vec<CatalogEntry>> {
    let mut statement = connection.prepare("SELECT catalog.hash, catalog.path FROM catalog WHERE catalog.hash in (SELECT hash FROM catalog GROUP BY hash HAVING COUNT(path) > 1 ORDER BY hash)")?;
    query(&mut statement, [])
}

pub(crate) fn foreach_entry<F>(connection: &Connection, mut f: F) -> Result<usize>
where
    F: FnMut(CatalogEntry) -> Result<()>,
{
    let mut statement = connection.prepare("SELECT catalog.hash, catalog.path FROM catalog")?;
    let entries = query(&mut statement, [])?;
    let mut count = 0;
    let mut errors = vec![];
    for entry in entries {
        match f(entry) {
            Ok(()) => count += 1,
            Err(e) => errors.push(e.to_string()),
        }
    }
    if errors.len() == 0 {
        Ok(count)
    } else {
        Err(eyre!(errors.join("\n")))
    }
}

pub(crate) fn find_already_imported(connection: &Connection) -> Result<Vec<CatalogEntry>> {
    let mut statement = connection.prepare(
        "SELECT catalog.hash, catalog.path FROM catalog, library WHERE catalog.hash = library.hash",
    )?;
    query(&mut statement, [])
}

fn query<T: Params>(statement: &mut Statement, params: T) -> Result<Vec<CatalogEntry>> {
    let result = statement
        .query_map(params, |r| CatalogEntry::try_from(r))?
        .into_iter()
        .collect::<Result<Vec<CatalogEntry>, rusqlite::Error>>()?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use eyre::eyre;
    use rusqlite::{params, Connection};

    use crate::database::{
        catalog::{catalog_insert_all, foreach_entry, query},
        library::persist_library_entries,
        library_entry::LibraryEntry,
        migrate,
    };

    use super::{
        find_already_imported, persist_catalog_entries, select_from_catalog, CatalogEntry,
    };

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

    fn some_entries() -> Vec<CatalogEntry> {
        vec![
            CatalogEntry {
                sha256: "1".to_string(),
                path: "a/a".to_string(),
            },
            CatalogEntry {
                sha256: "2".to_string(),
                path: "a/b".to_string(),
            },
        ]
    }

    #[test]
    fn catalog_insert_all_returns_count_of_insertions() {
        let entries = some_entries();
        let mut connection = new_database();
        let mut transaction = connection.transaction().unwrap();

        assert_eq!(
            entries.len(),
            catalog_insert_all(&mut transaction, &entries).unwrap()
        );
    }

    #[test]
    fn catalog_insert_all_inserts_into_the_catalog_table() {
        let entries = some_entries();

        let mut connection = new_database();
        let mut transaction = connection.transaction().unwrap();

        catalog_insert_all(&mut transaction, &entries).unwrap();
        transaction.commit().unwrap();

        assert!(catalog_contains(&mut connection, &entries[0]));
        assert!(catalog_contains(&mut connection, &entries[1]));
    }

    #[test]
    fn catalog_insert_all_results_in_error_when_one_is_duplicate() {
        let entries = some_entries();

        let mut connection = new_database();
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
        let entries = some_entries();
        let mut connection = new_database();
        connection
            .execute(
                "INSERT INTO catalog (hash, path) values (?1, ?2)",
                params!(&entries[0].sha256, &entries[0].path),
            )
            .unwrap();
        let result = persist_catalog_entries(&mut connection, &entries);
        assert!(!catalog_contains(
            &mut connection,
            &CatalogEntry {
                sha256: "2".to_string(),
                path: "b".to_string(),
            }
        ));
        assert_eq!(
            result.err().unwrap().to_string(),
            "Failed to insert (1, a/a): UNIQUE constraint failed: catalog.path".to_string()
        );
    }

    #[test]
    fn select_from_catalog_returns_the_catalog_entries() {
        let entries = some_entries();
        let mut connection = new_database();

        persist_catalog_entries(&mut connection, &entries).unwrap();
        let results = select_from_catalog(&connection, "a").unwrap();
        assert_eq!(entries, results);
    }

    #[test]
    fn select_from_catalog_does_not_return_entries_in_library() {
        let entries = some_entries();
        let mut connection = new_database();

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
        let mut connection = new_database();

        persist_catalog_entries(&mut connection, &entries).unwrap();
        let results = select_from_catalog(&connection, "a").unwrap();
        assert_eq!(1, results.len());
    }

    #[test]
    fn foreach_entry_applies_the_function_to_each_entry() {
        let entries = some_entries();

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
    fn foreach_entry_returns_error_when_parameter_function_does() {
        let entries = some_entries();

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
    fn foreach_entry_returns_error_when_statement_is_incorrect() {
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
    fn query_returns_error_when_row_cannot_be_converted_to_entry() {
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

        let mut statement = connection
            .prepare("SELECT catalog.hash, catalog.path FROM catalog")
            .unwrap();

        assert_eq!(
            "Invalid column type Integer at index: 0, name: hash",
            query(&mut statement, []).err().unwrap().to_string()
        )
    }

    #[test]
    fn find_already_imported_returns_all_catalog_entries_also_in_library() {
        let mut entries = some_entries();
        entries.push(CatalogEntry::new(
            entries[0].sha256.to_owned(),
            "c/cc".to_string(),
        ));

        let mut connection = new_database_containing(&entries);
        persist_library_entries(
            &mut connection,
            &vec![LibraryEntry::new(
                entries[0].sha256.to_owned(),
                PathBuf::from("a/aa"),
            )],
        )
        .unwrap();

        let result = find_already_imported(&connection).unwrap();
        assert_eq!(2, result.len());
        assert_eq!(entries[0], result[0]);
        assert_eq!(entries[2], result[1]);
    }

    #[test]
    fn find_already_imported_does_not_return_catalog_entries_with_no_matching_library_entries() {
        let entries = some_entries();

        let mut connection = new_database_containing(&entries);
        persist_library_entries(
            &mut connection,
            &vec![LibraryEntry::new("12".to_string(), PathBuf::from("a/aa"))],
        )
        .unwrap();

        assert!(find_already_imported(&connection).unwrap().is_empty());
    }
}
