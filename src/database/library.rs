use eyre::{eyre, Result};
use rusqlite::{Connection, Transaction};

use super::library_entry::LibraryEntry;

pub(crate) fn persist_library_entries(
    connection: &mut Connection,
    entries: &Vec<LibraryEntry>,
) -> Result<usize> {
    let mut transaction = connection.transaction()?;
    match library_insert_all(&mut transaction, entries) {
        Ok(db_count) if db_count == entries.len() => {
            transaction.commit()?;
            Ok(db_count)
        }
        _ => Err(eyre!(
            "Database insert count does not match files copied. Files left in place."
        )),
    }
}

fn library_insert_all(transaction: &mut Transaction, entries: &Vec<LibraryEntry>) -> Result<usize> {
    let mut count = 0;
    let mut statement = transaction.prepare("INSERT INTO library (hash, path) values (?1, ?2)")?;
    for entry in entries {
        count += statement
            .execute([entry.sha256(), &entry.path().to_string_lossy().to_string()])
            .map_err(|e| {
                eyre!(
                    "Failed to insert ({}, {}): {}",
                    &entry.sha256(),
                    &entry.path().display(),
                    e
                )
            })?;
    }
    Ok(count)
}

pub(crate) fn foreach_entry<F>(connection: &Connection, mut f: F) -> Result<usize>
where
    F: FnMut(LibraryEntry) -> Result<()>,
{
    let mut query = connection.prepare("SELECT hash, path FROM library")?;
    let entries = query.query_map([], |r| {
        Ok(LibraryEntry::new(
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
    use eyre::eyre;
    use std::path::PathBuf;

    use rusqlite::{params, Connection};

    use crate::database::{
        library::{library_insert_all, LibraryEntry},
        migrate,
    };

    use super::{foreach_entry, persist_library_entries};

    fn library_contains(connection: &mut Connection, entry: &LibraryEntry) -> bool {
        match connection.query_row(
            "SELECT true FROM library WHERE hash = ?1 AND path = ?2",
            params!(entry.sha256(), entry.path().to_string_lossy().to_string()),
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

    fn new_database_containing(entries: &Vec<LibraryEntry>) -> Connection {
        let mut connection = new_database();
        let mut transaction = connection.transaction().unwrap();
        let _count = library_insert_all(&mut transaction, entries).unwrap();
        transaction.commit().unwrap();
        connection
    }

    #[test]
    fn library_insert_all_returns_count_of_insertions() {
        let mut connection = new_database();
        let mut transaction = connection.transaction().unwrap();
        let entries = vec![
            LibraryEntry {
                sha256: "1".to_string(),
                path: PathBuf::from("a"),
            },
            LibraryEntry {
                sha256: "2".to_string(),
                path: PathBuf::from("b"),
            },
        ];
        assert_eq!(
            entries.len(),
            library_insert_all(&mut transaction, &entries).unwrap()
        );
    }

    #[test]
    fn library_insert_all_inserts_into_the_library_table() {
        let mut connection = new_database();

        let mut transaction = connection.transaction().unwrap();
        let entries: Vec<_> = vec![
            LibraryEntry {
                sha256: "1".to_string(),
                path: PathBuf::from("a"),
            },
            LibraryEntry {
                sha256: "2".to_string(),
                path: PathBuf::from("b"),
            },
        ];
        library_insert_all(&mut transaction, &entries).unwrap();
        transaction.commit().unwrap();

        assert!(library_contains(&mut connection, &entries[0]));
        assert!(library_contains(&mut connection, &entries[1]));
    }

    #[test]
    fn library_insert_all_results_in_error_when_one_is_duplicate() {
        let mut connection = new_database();
        let entries = vec![
            LibraryEntry {
                sha256: "1".to_string(),
                path: PathBuf::from("a"),
            },
            LibraryEntry {
                sha256: "2".to_string(),
                path: PathBuf::from("b"),
            },
        ];
        connection
            .execute(
                "INSERT INTO library (hash, path) values (?1, ?2)",
                params!(
                    &entries[0].sha256(),
                    &entries[0].path().to_string_lossy().to_string()
                ),
            )
            .unwrap();
        let mut transaction = connection.transaction().unwrap();
        assert!(library_insert_all(&mut transaction, &entries).is_err());
        transaction.rollback().unwrap();
        assert!(!library_contains(&mut connection, &entries[1]));
    }

    #[test]
    fn persist_library_entries_rollbacks_when_insert_fails() {
        let mut connection = new_database();
        let entries = vec![
            LibraryEntry {
                sha256: "1".to_string(),
                path: PathBuf::from("a"),
            },
            LibraryEntry {
                sha256: "2".to_string(),
                path: PathBuf::from("b"),
            },
        ];
        connection
            .execute(
                "INSERT INTO library (hash, path) values (?1, ?2)",
                params!(
                    &entries[0].sha256(),
                    &entries[0].path().to_string_lossy().to_string()
                ),
            )
            .unwrap();

        let result = persist_library_entries(&mut connection, &entries);

        assert!(library_contains(
            &mut connection,
            &LibraryEntry {
                sha256: "1".to_string(),
                path: PathBuf::from("a"),
            }
        ));
        assert!(!library_contains(
            &mut connection,
            &LibraryEntry {
                sha256: "2".to_string(),
                path: PathBuf::from("b"),
            }
        ));
        assert_eq!(
            result.err().unwrap().to_string(),
            "Database insert count does not match files copied. Files left in place.".to_string()
        );
    }

    #[test]
    fn foreach_entry_applies_the_function_to_each_entry() {
        let entries = vec![
            LibraryEntry {
                sha256: "1".to_string(),
                path: PathBuf::from("a"),
            },
            LibraryEntry {
                sha256: "2".to_string(),
                path: PathBuf::from("b"),
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
            "no such table: library",
            foreach_entry(&connection, |_e| Ok(()))
                .err()
                .unwrap()
                .to_string()
        )
    }

    #[test]
    fn foreach_entry_returns_error_when_parameter_function_does() {
        let entries = vec![
            LibraryEntry {
                sha256: "1".to_string(),
                path: PathBuf::from("a"),
            },
            LibraryEntry {
                sha256: "2".to_string(),
                path: PathBuf::from("b"),
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
            .execute("create table library (hash integer, path string)", [])
            .unwrap();
        connection
            .execute(
                "INSERT INTO library (hash, path) values (?1, ?2)",
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
