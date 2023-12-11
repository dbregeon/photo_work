use std::{
    fs::{copy, create_dir_all},
    path::PathBuf,
};

use clap::{arg, ArgMatches, Command};
use eyre::{eyre, Result};
use rusqlite::Connection;

use crate::{
    clapext::SubApplication,
    database::{
        self, catalog::select_from_catalog, library::library_insert_all,
        library_entry::LibraryEntry,
    },
};

const IMPORT: &str = "import";

pub(crate) struct Import;

impl SubApplication for Import {
    fn name(&self) -> &'static str {
        IMPORT
    }

    fn command(&self) -> Command {
        Command::new(self.name())
            .about("Imports cataloged pictures that are not already in the library")
            .arg(arg!(<PATH_PREFIX> "The prefix to the path queried in the catalog"))
            .arg_required_else_help(true)
    }

    fn handle(&self, sub_matches: &ArgMatches) -> Result<()> {
        let prefix = sub_matches
            .get_one::<String>("PATH_PREFIX")
            .expect("required")
            .as_str();
        let db_path: PathBuf = [".photo_works", "db.db3"].iter().collect();
        let connection = database::open(&db_path)?;

        println!(
            "Importing from catalog images where path starts with {}",
            &prefix
        );

        Ok(println!(
            "Imported {} pictures",
            import(connection, prefix)?
        ))
    }
}

fn import(connection: Connection, path_prefix: &str) -> Result<usize> {
    let library_entries = select_from_catalog(&connection, path_prefix)?
        .iter()
        .map(|e| {
            LibraryEntry::try_from(e)
                .and_then(|p| try_copy_catalog_entry(&e.path(), p, &e.sha256()))
        })
        .filter_map(|r| match r {
            Ok(library_entry) => Some(library_entry),
            Err(e) => {
                println!("{}", e);
                None
            }
        })
        .collect::<Vec<LibraryEntry>>();
    persist_library_entries(connection, library_entries)
}

fn copy_catalog_entry(
    from: &PathBuf,
    library_entry: LibraryEntry,
    hash: &str,
) -> Result<LibraryEntry> {
    if let Some(dirname) = &library_entry.path().parent() {
        create_dir_all(&dirname)?;
    }
    copy(from, library_entry.path())?;
    if &hash != &library_entry.sha256() {
        Err(eyre!(
            "{} sha256 does not match copied {}. Aborting.",
            from.display(),
            &library_entry.path().display()
        ))
    } else {
        Ok(library_entry)
    }
}

fn persist_library_entries(
    mut connection: Connection,
    entries: Vec<LibraryEntry>,
) -> Result<usize> {
    let mut transaction = connection.transaction()?;
    let db_count = library_insert_all(&mut transaction, &entries)?;
    if db_count == entries.len() {
        transaction.commit()?;
        Ok(db_count)
    } else {
        transaction.rollback()?;
        Err(eyre!(
            "Database insert count does not match files copied. Files left in place."
        ))
    }
}

fn try_copy_catalog_entry(
    path: &PathBuf,
    library_entry: LibraryEntry,
    hash: &str,
) -> Result<LibraryEntry> {
    println!(
        "Importing {} into {}",
        path.display(),
        library_entry.path().display()
    );
    let exists = library_entry.path().exists();
    if exists {
        Err(eyre!("{} already exists.", library_entry.path().display()))
    } else {
        copy_catalog_entry(path, library_entry, hash)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::remove_file, path::PathBuf};

    use crate::{
        command::import::try_copy_catalog_entry,
        database::{catalog::CatalogEntry, common::sha256_digest, library_entry::LibraryEntry},
    };

    use super::copy_catalog_entry;

    #[test]
    fn copy_catalog_entry_copies_file() {
        let from = &PathBuf::from("Cargo.toml");
        let catalog_entry = CatalogEntry::try_from(&given_at_target_path("test_file.txt")).unwrap();
        let to = LibraryEntry::try_from(&catalog_entry).unwrap();
        let hash = &sha256_digest(from).unwrap();
        let path = to.path().clone();

        copy_catalog_entry(from, to, hash).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn copy_catalog_entry_returns_err_when_hashes_dont_match() {
        let from = &PathBuf::from("Cargo.toml");
        let catalog_entry =
            CatalogEntry::try_from(&given_at_target_path("copy_catalog_entry.txt")).unwrap();
        let to = LibraryEntry::try_from(&catalog_entry).unwrap();
        let hash = &sha256_digest(&PathBuf::from("Cargo.lock")).unwrap();

        assert!(copy_catalog_entry(from, to, hash).is_err());
    }

    #[test]
    fn try_copy_catalog_entry_copies_file() {
        let from = &PathBuf::from("Cargo.toml");
        let catalog_entry =
            CatalogEntry::try_from(&given_at_target_path("try_copy_catalog_entry.txt")).unwrap();
        let to = LibraryEntry::try_from(&catalog_entry).unwrap();
        let hash = &sha256_digest(from).unwrap();
        let path = to.path().clone();

        try_copy_catalog_entry(from, to, hash).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn try_copy_catalog_entry_returns_err_when_path_already_exists() {
        let from = &PathBuf::from("Cargo.toml");
        let catalog_entry = CatalogEntry::try_from(&given_at_target_path("Cargo.lock")).unwrap();
        let to = LibraryEntry::try_from(&catalog_entry).unwrap();
        let hash = &sha256_digest(&PathBuf::from("Cargo.lock")).unwrap();

        assert!(try_copy_catalog_entry(from, to, hash).is_err());
    }

    fn given_at_target_path(filename: &str) -> PathBuf {
        let to = ["target", "tmp", filename].iter().collect();
        let _ = remove_file(&to);
        to
    }
}
