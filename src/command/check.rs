use std::{path::PathBuf, time::Instant};

use clap::{ArgMatches, Command};
use eyre::{eyre, Result};
use rusqlite::Connection;

use crate::{
    clapext::SubApplication,
    database::{self, common::sha256_digest},
};

const CHECK: &str = "check";

pub(crate) struct Check;

impl SubApplication for Check {
    fn name(&self) -> &'static str {
        CHECK
    }

    fn command(&self) -> Command {
        Command::new(self.name())
            .about("Verifies the content of the library and of the catalog")
            .subcommand_required(true)
            .arg_required_else_help(true)
            .subcommands([
                Command::new("library").about("Verify the integrity of the library."),
                Command::new("catalog").about("Verify the integrity of the catalog."),
                Command::new("duplicates").about("Reports duplicate pictures in catalog."),
                Command::new("imported").about("Reports catalog entries already in the library."),
            ])
    }

    fn handle(&self, sub_matches: &ArgMatches) -> Result<()> {
        let db_path: PathBuf = [".photo_works", "db.db3"].iter().collect();
        let connection = database::open(&db_path)?;

        match sub_matches.subcommand() {
            Some((name, _sub_matches)) => match name {
                "library" => check_library_integrity(&connection),
                "catalog" => check_catalog_integrity(&connection),
                "duplicates" => check_catalog_duplicates(&connection),
                "imported" => check_imported_library_entries(&connection),
                _ => unreachable!("Unknown subcommand"),
            },
            None => unreachable!("Missing subcommand."),
        }
    }
}

fn check_catalog_integrity(connection: &Connection) -> Result<()> {
    println!("Checking catalog images",);
    let catalog_check_start = Instant::now();

    let result = crate::database::catalog::foreach_entry(connection, |e| {
        if e.sha256().to_string() == sha256_digest(&e.path())? {
            Ok(())
        } else {
            Err(eyre!(
                "Failed catalog check for {}",
                &e.path().to_string_lossy().to_string()
            ))
        }
    })?;
    Ok(println!(
        "Checked {} pictures in {} seconds",
        result,
        catalog_check_start.elapsed().as_secs()
    ))
}

fn check_library_integrity(connection: &Connection) -> Result<()> {
    println!("Checking library images");
    let library_check_start = Instant::now();

    let result = crate::database::library::foreach_entry(connection, |e| {
        if e.sha256().to_string() == sha256_digest(&e.path())? {
            Ok(())
        } else {
            Err(eyre!(
                "Failed library check for {}",
                &e.path().to_string_lossy().to_string()
            ))
        }
    })?;
    Ok(println!(
        "Checked {} pictures in {} seconds",
        result,
        library_check_start.elapsed().as_secs()
    ))
}

fn check_catalog_duplicates(connection: &Connection) -> Result<()> {
    println!("Checking catalog duplicates");
    let catalog_check_start = Instant::now();

    let result = crate::database::catalog::find_duplicates(connection)?;
    if result.len() == 0 {
        Ok(println!(
            "No duplicates found. {} seconds.",
            catalog_check_start.elapsed().as_secs()
        ))
    } else {
        Ok(println!(
            "{} duplicates found. {} seconds. Paths:\n{}",
            result.len(),
            catalog_check_start.elapsed().as_secs(),
            result
                .iter()
                .map(|(key, values)| format!(
                    "{}:\n{}",
                    key,
                    values
                        .iter()
                        .map(|e| e.path().to_string_lossy().to_string())
                        .collect::<Vec<String>>()
                        .join("\n")
                ))
                .collect::<Vec<String>>()
                .join("\n")
        ))
    }
}

fn check_imported_library_entries(connection: &Connection) -> Result<()> {
    println!("Checking already imported entries still in catalog");
    let catalog_check_start = Instant::now();

    let result = crate::database::catalog::find_already_imported(connection)?;
    if result.len() == 0 {
        Ok(println!(
            "No duplicate entries between library and catalog. {} seconds.",
            catalog_check_start.elapsed().as_secs()
        ))
    } else {
        Ok(println!(
            "{} entries found in both catalog and library. {} seconds. Paths:\n{}",
            result.len(),
            catalog_check_start.elapsed().as_secs(),
            result
                .iter()
                .map(|c| format!("{}: {}", c.sha256(), c.path().to_string_lossy().to_string()))
                .collect::<Vec<String>>()
                .join("\n")
        ))
    }
}

#[cfg(test)]
mod tests {}
