use std::{ffi::OsStr, fs::canonicalize, path::PathBuf};

use clap::{arg, ArgMatches, Command};
use eyre::Result;
use rusqlite::Connection;
use walkdir::WalkDir;

use crate::{
    clapext::SubApplication,
    database::catalog::{persist_catalog_entries, CatalogEntry},
};

const CATALOG: &str = "catalog";

pub(crate) struct Catalog;

impl SubApplication for Catalog {
    fn name(&self) -> &'static str {
        CATALOG
    }

    fn command(&self) -> Command {
        Command::new(self.name())
            .about("Catalogs a directory in a photo_works database")
            .arg(arg!(<PATH> "The path to catalog"))
            .arg_required_else_help(true)
    }

    fn handle(&self, sub_matches: &ArgMatches) -> Result<()> {
        let path = canonicalize(
            sub_matches
                .get_one::<String>("PATH")
                .expect("required")
                .as_str(),
        )?;
        let db_path: PathBuf = [".photo_works", "db.db3"].iter().collect();
        let connection = Connection::open(db_path)?;

        println!("Cataloging {}", path.to_string_lossy());

        Ok(println!(
            "Cataloged {} pictures",
            catalog(connection, &path)?
        ))
    }
}

fn catalog(mut connection: Connection, path: &PathBuf) -> Result<usize> {
    let entries = WalkDir::new(&PathBuf::from(path))
        .into_iter()
        .filter_entry(|e| !is_hidden_file_name(e.file_name()))
        .filter_map(|e| e.ok())
        .map(|dir_entry| {
            let entry_path = &dir_entry.into_path();
            let catalog_entry = entry_path.try_into();
            if catalog_entry.is_err() {
                println!("Failed to process {}", entry_path.display())
            }
            catalog_entry
        })
        .filter_map(|e: Result<CatalogEntry, eyre::Error>| e.ok())
        .collect::<Vec<CatalogEntry>>();
    persist_catalog_entries(&mut connection, entries)
}

/// Returns true when a file_name starts with '.'
fn is_hidden_file_name(file_name: &OsStr) -> bool {
    let bytes = file_name.as_encoded_bytes();
    bytes.len() >= 2 && bytes[0] == b'.' && bytes[1] != b'.'
}

#[cfg(test)]
mod tests {
    use crate::command::catalog::is_hidden_file_name;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    #[test]
    fn is_hidden_file_name_is_false_for_empty_string() {
        assert!(!is_hidden_file_name(&OsStr::from_bytes(&[])))
    }

    #[test]
    fn is_hidden_file_name_is_false_for_dot() {
        assert!(!is_hidden_file_name(&OsStr::from_bytes(&[b'.'])))
    }

    #[test]
    fn is_hidden_file_name_is_false_for_single_non_dot() {
        assert!(!is_hidden_file_name(&OsStr::from_bytes(&[b' '])))
    }

    #[test]
    fn is_hidden_file_name_is_false_for_dot_dot() {
        assert!(!is_hidden_file_name(&OsStr::from_bytes(&[b'.', b'.'])))
    }

    #[test]
    fn is_hidden_file_name_is_false_for_string_not_starting_with_dot() {
        assert!(!is_hidden_file_name(&OsStr::from_bytes(&[b' ', b'.'])))
    }

    #[test]
    fn is_hidden_file_name_is_true_for_string_starting_with_dot() {
        assert!(is_hidden_file_name(&OsStr::from_bytes(&[b'.', b' ', b'a'])))
    }
}
