use rusqlite::{params, Connection};

use super::{
    catalog::persist_catalog_entries, catalog_entry::CatalogEntry,
    library::persist_library_entries, library_entry::LibraryEntry, migrate,
};

pub fn new_database() -> Connection {
    let mut connection = new_connection();
    migrate(&mut connection).unwrap();
    connection
}

pub fn new_connection() -> Connection {
    Connection::open_in_memory().unwrap()
}

pub fn new_database_containing_catalog_entries(entries: &Vec<CatalogEntry>) -> Connection {
    let mut connection = new_database();
    let _count = persist_catalog_entries(&mut connection, entries);
    connection
}

pub fn new_database_containing_library_entries(entries: &Vec<LibraryEntry>) -> Connection {
    let mut connection = new_database();
    let _count = persist_library_entries(&mut connection, entries).unwrap();
    connection
}

pub fn catalog_contains(connection: &mut Connection, entry: &CatalogEntry) -> bool {
    match connection.query_row(
        "SELECT true FROM catalog WHERE hash = ?1 AND path = ?2",
        params!(entry.sha256, entry.path),
        |row| row.get::<_, bool>(0),
    ) {
        Ok(b) => b,
        Err(_) => false,
    }
}
