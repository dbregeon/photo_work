use std::path::PathBuf;

use eyre::Result;
use refinery::{Error, Report};
use rusqlite::Connection;

pub(crate) mod catalog;
pub(crate) mod common;
pub(crate) mod library;
pub(crate) mod library_entry;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations");
}

pub(crate) fn open(db: &PathBuf) -> Result<Connection> {
    let mut connection = Connection::open(&db)?;
    migrate(&mut connection)?;
    Ok(connection)
}

fn migrate(connection: &mut Connection) -> Result<Report, Error> {
    embedded::migrations::runner().run(connection)
}

#[cfg(test)]
mod tests {
    use rusqlite::{params, Connection};

    use crate::database::migrate;

    fn table_exists(connection: &mut Connection, table_name: &str) -> bool {
        let mut statement = connection
            .prepare("SELECT count(name) FROM sqlite_master WHERE type='table' AND name=?1")
            .unwrap();
        let mut rows = statement.query(params!(table_name)).unwrap();
        let count = rows.next().unwrap().unwrap();

        1 == count.get::<_, usize>(0).unwrap()
    }

    #[test]
    fn migrate_creates_the_catalog_table() {
        let mut connection = Connection::open_in_memory().unwrap();
        assert!(migrate(&mut connection).is_ok());
        assert!(table_exists(&mut connection, "catalog"));
    }

    #[test]
    fn migrate_creates_the_library_table() {
        let mut connection = Connection::open_in_memory().unwrap();
        assert!(migrate(&mut connection).is_ok());
        assert!(table_exists(&mut connection, "library"));
    }
}
