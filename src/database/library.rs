use eyre::{eyre, Result};
use rusqlite::Transaction;

use super::library_entry::LibraryEntry;

pub(crate) fn library_insert_all(
    transaction: &mut Transaction,
    entries: &Vec<LibraryEntry>,
) -> Result<usize> {
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rusqlite::{params, Connection};

    use crate::database::{
        library::{library_insert_all, LibraryEntry},
        migrate,
    };

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

    fn connection() -> Connection {
        let mut connection = Connection::open_in_memory().unwrap();
        migrate(&mut connection).unwrap();
        connection
    }

    #[test]
    fn library_insert_all_returns_count_of_insertions() {
        let mut connection = connection();
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
        let mut connection = connection();

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
        let mut connection = connection();
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
}
