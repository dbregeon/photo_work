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
        self, catalog::select_from_catalog, common::sha256_digest,
        library::persist_library_entries, library_entry::LibraryEntry,
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

fn import(mut connection: Connection, path_prefix: &str) -> Result<usize> {
    let library_entries = select_from_catalog(&connection, path_prefix)?
        .iter()
        .map(|e| LibraryEntry::try_from(e).and_then(|p| try_copy_catalog_entry(&e.path(), p)))
        .filter_map(|r| match r {
            Ok(library_entry) => Some(library_entry),
            Err(e) => {
                println!("{}", e);
                None
            }
        })
        .collect::<Vec<LibraryEntry>>();
    persist_library_entries(&mut connection, library_entries)
}

fn try_copy_catalog_entry(path: &PathBuf, library_entry: LibraryEntry) -> Result<LibraryEntry> {
    println!(
        "Importing {} into {}",
        path.display(),
        library_entry.path().display()
    );
    let exists = library_entry.path().exists();
    if exists {
        Err(eyre!("{} already exists.", library_entry.path().display()))
    } else {
        copy_catalog_entry(path, library_entry)
    }
}

fn copy_catalog_entry(from: &PathBuf, library_entry: LibraryEntry) -> Result<LibraryEntry> {
    if let Some(dirname) = &library_entry.path().parent() {
        create_dir_all(&dirname)?;
    }
    copy(from, library_entry.path())?;
    let copy_sha256 = sha256_digest(&library_entry.path())?;
    if &library_entry.sha256() != &copy_sha256 {
        Err(eyre!(
            "{} sha256 does not match copied {}. Aborting.",
            from.display(),
            &library_entry.path().display()
        ))
    } else {
        Ok(library_entry)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::remove_file, path::PathBuf};

    use serial_test::serial;

    use crate::{
        command::import::try_copy_catalog_entry,
        database::{catalog::CatalogEntry, library_entry::LibraryEntry},
    };

    use super::copy_catalog_entry;

    #[test]
    #[serial]
    fn copy_catalog_entry_copies_file() {
        let catalog_entry =
            CatalogEntry::try_from(&given_a_path_for_an_image_with_original_date()).unwrap();
        let to = LibraryEntry::try_from(&catalog_entry).unwrap();
        let path = to.path().clone();

        copy_catalog_entry(&catalog_entry.path(), to).unwrap();
        assert!(path.exists());

        remove_file(&path).unwrap();
    }

    #[test]
    fn copy_catalog_entry_returns_err_when_hashes_dont_match() {
        let from = &PathBuf::from("Cargo.toml");
        let actual_library_entry = LibraryEntry::try_from(
            &CatalogEntry::try_from(&given_a_path_for_an_image_with_original_date()).unwrap(),
        )
        .unwrap();
        let erroneous_entry =
            LibraryEntry::new("1234".to_string(), actual_library_entry.path().to_owned());

        let error = copy_catalog_entry(from, erroneous_entry)
            .err()
            .unwrap()
            .to_string();
        assert_eq!(
            error,
            format!(
                "Cargo.toml sha256 does not match copied {}. Aborting.",
                actual_library_entry.path().to_string_lossy().to_string()
            )
        );
    }

    #[test]
    #[serial]
    fn try_copy_catalog_entry_copies_file() {
        let from = given_a_path_for_an_image_with_original_date();
        let catalog_entry = CatalogEntry::try_from(&from).unwrap();
        let to = LibraryEntry::try_from(&catalog_entry).unwrap();
        let path = to.path().clone();

        let _ = remove_file(&path);

        try_copy_catalog_entry(&from, to).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn try_copy_catalog_entry_returns_err_when_path_already_exists() {
        let from = &PathBuf::from("Cargo.toml");
        let to = LibraryEntry::new("1234".to_string(), PathBuf::from("Cargo.toml"));

        let error = try_copy_catalog_entry(from, to).err().unwrap().to_string();
        assert_eq!(error, "Cargo.toml already exists.");
    }

    fn given_a_path_for_an_image_with_original_date() -> PathBuf {
        ["resources", "test", "kami_neko.jpeg"].iter().collect()
    }
}
