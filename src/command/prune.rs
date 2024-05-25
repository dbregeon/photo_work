use std::{fs::remove_file, path::PathBuf, time::Instant};

use clap::{ArgMatches, Command};
use eyre::{eyre, Result};
use rusqlite::Connection;

use crate::{
    clapext::SubApplication,
    database::{
        self,
        catalog::{find_already_imported, find_duplicates},
        catalog_entry::CatalogEntry,
    },
};

const PRUNE: &str = "prune";

pub(crate) struct Prune;

impl SubApplication for Prune {
    fn name(&self) -> &'static str {
        PRUNE
    }

    fn command(&self) -> Command {
        Command::new(self.name())
            .about("Remmoves the catalog contents that are no longer needed")
            .subcommand_required(true)
            .arg_required_else_help(true)
            .subcommands([
                Command::new("duplicates")
                    .about("Moves duplicate pictures found in catalog to the trash."),
                Command::new("imported")
                    .about("Moves catalog entries already in the library to the trash."),
            ])
    }

    fn handle(&self, sub_matches: &ArgMatches) -> Result<()> {
        let db_path: PathBuf = [".photo_works", "db.db3"].iter().collect();
        let mut connection = database::open(&db_path)?;

        match sub_matches.subcommand() {
            Some((name, _sub_matches)) => match name {
                "duplicates" => prune_catalog_duplicates(&mut connection),
                "imported" => prune_imported_catalog_entries(&mut connection),
                _ => unreachable!("Unknown subcommand"),
            },
            None => unreachable!("Missing subcommand."),
        }
    }
}

fn prune_catalog_duplicates(mut connection: &mut Connection) -> Result<()> {
    println!("Pruning catalog duplicates");
    let catalog_prune_start = Instant::now();

    let duplicates = find_duplicates(connection)?;
    if duplicates.len() == 0 {
        Ok(println!(
            "No duplicates found. {} seconds.",
            catalog_prune_start.elapsed().as_secs()
        ))
    } else {
        let mut count = 0;
        for dupes in duplicates.values() {
            for duplicate in dupes.iter().skip(1) {
                count += 1;
                move_to_trash(duplicate)?
            }
        }
        database::catalog::remove_catalog_entries(
            &mut connection,
            &duplicates
                .into_values()
                .flat_map(|v| v.into_iter().skip(1))
                .collect(),
        )?;
        Ok(println!(
            "{} duplicates moved to trash. {} seconds.",
            count,
            catalog_prune_start.elapsed().as_secs(),
        ))
    }
}

fn prune_imported_catalog_entries(mut connection: &mut Connection) -> Result<()> {
    println!("Pruning imported catalog entries");
    let catalog_prune_start = Instant::now();

    let already_imported = find_already_imported(connection)?;
    if already_imported.len() == 0 {
        Ok(println!(
            "No imported entries found. {} seconds.",
            catalog_prune_start.elapsed().as_secs()
        ))
    } else {
        let mut count = 0;
        for entry in &already_imported {
            count += 1;
            move_to_trash(entry)?
        }
        database::catalog::remove_catalog_entries(&mut connection, &already_imported)?;
        Ok(println!(
            "{} imported entries moved to trash. {} seconds.",
            count,
            catalog_prune_start.elapsed().as_secs(),
        ))
    }
}

fn move_to_trash(entry: &CatalogEntry) -> Result<()> {
    let original_path = entry.path();
    let trash_path = trash_path(entry)?;
    let trash_dir = trash_path.parent().ok_or(eyre!("Invalid Directory"))?;
    std::fs::create_dir_all(trash_dir)?;
    std::fs::copy(&original_path, trash_path)?;
    Ok(remove_file(original_path)?)
}

fn trash_path(entry: &CatalogEntry) -> Result<PathBuf> {
    let original_path = entry.path();
    let directory = original_path
        .parent()
        .and_then(|p| p.file_name())
        .ok_or(eyre!("Invalid File"))?;
    let filename = original_path.file_name().ok_or(eyre!("Invalid File"))?;
    let mut trash_path = PathBuf::from(".trash");
    trash_path.push(directory);
    trash_path.push(filename);
    Ok(trash_path)
}

#[cfg(test)]
mod tests {
    use std::fs::remove_dir_all;

    use tempfile::NamedTempFile;

    use crate::{
        command::prune::prune_catalog_duplicates,
        database::{
            catalog_entry::CatalogEntry,
            library_entry::LibraryEntry,
            test_utils::{
                catalog_contains, library_contains,
                new_database_containing_catalog_and_library_entries,
                new_database_containing_catalog_entries,
            },
        },
    };

    use super::{move_to_trash, prune_imported_catalog_entries, trash_path};

    #[test]
    fn prune_catalog_duplicates_entries_removes_the_entry() {
        let catalog_entry1 = NamedTempFile::new().unwrap();
        let catalog_entry2 = NamedTempFile::new().unwrap();
        let catalog_entry3 = NamedTempFile::new().unwrap();

        let entries = vec![
            CatalogEntry::new(
                "1234".to_string(),
                catalog_entry1.path().to_string_lossy().to_string(),
            ),
            CatalogEntry::new(
                "1235".to_string(),
                catalog_entry2.path().to_string_lossy().to_string(),
            ),
            CatalogEntry::new(
                "1234".to_string(),
                catalog_entry3.path().to_string_lossy().to_string(),
            ),
        ];
        let mut connection = new_database_containing_catalog_entries(&entries);
        prune_catalog_duplicates(&mut connection).unwrap();

        assert!(!catalog_contains(&mut connection, &entries[2]));

        assert!(catalog_contains(&mut connection, &entries[0]));
        assert!(catalog_contains(&mut connection, &entries[1]));
    }

    #[test]
    fn prune_imported_catalog_entries_removes_the_entry() {
        let catalog_entry1 = NamedTempFile::new().unwrap();
        let catalog_entry2 = NamedTempFile::new().unwrap();
        let library_entry1 = NamedTempFile::new().unwrap();

        let catalog_entries = vec![
            CatalogEntry::new(
                "1234".to_string(),
                catalog_entry1.path().to_string_lossy().to_string(),
            ),
            CatalogEntry::new(
                "1235".to_string(),
                catalog_entry2.path().to_string_lossy().to_string(),
            ),
        ];
        let library_entries = vec![LibraryEntry::new(
            "1234".to_string(),
            library_entry1.path().into(),
        )];

        let mut connection =
            new_database_containing_catalog_and_library_entries(&catalog_entries, &library_entries);
        prune_imported_catalog_entries(&mut connection).unwrap();

        assert!(!catalog_contains(&mut connection, &catalog_entries[0]));

        assert!(catalog_contains(&mut connection, &catalog_entries[1]));
        assert!(library_contains(&mut connection, &library_entries[0]));
    }

    #[test]
    fn trash_path_prefixes_directory() {
        let entry = CatalogEntry::new("1234".to_string(), "a/b/c.png".to_string());
        let trash_path = trash_path(&entry).unwrap();
        assert_eq!(".trash/b/c.png", trash_path.to_string_lossy().to_string());
    }

    #[test]
    fn trash_path_fails_when_no_file_name() {
        let entry = CatalogEntry::new("1234".to_string(), "..".to_string());
        let error = trash_path(&entry).err().unwrap().to_string();
        assert_eq!("Invalid File", error);
    }

    #[test]
    fn trash_path_fails_when_no_parent_directory() {
        let entry = CatalogEntry::new("1234".to_string(), "c.png".to_string());
        let error = trash_path(&entry).err().unwrap().to_string();
        assert_eq!("Invalid File", error);
    }

    #[test]
    fn move_to_trash_moves_the_file() {
        let file = NamedTempFile::new().unwrap();
        let entry = CatalogEntry::new(
            "1234".to_string(),
            file.path().to_string_lossy().to_string(),
        );
        let trash_path = trash_path(&entry).unwrap();
        move_to_trash(&entry).unwrap();
        assert!(trash_path.exists());
        remove_dir_all(".trash").unwrap();
    }
}
