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
        Command::new(self.name()).about("Verifies the content of the library and of the catalog")
    }

    fn handle(&self, _sub_matches: &ArgMatches) -> Result<()> {
        let db_path: PathBuf = [".photo_works", "db.db3"].iter().collect();
        let connection = database::open(&db_path)?;

        //        check_catalog_integrity(&connection)?;
        //        check_library_integrity(&connection)?;
        check_catalog_duplicates(&connection)?;

        // todo check_prunables (in both library and catalog)

        Ok(())
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
                .map(|c| format!("{}: {}", c.sha256(), c.path().to_string_lossy().to_string()))
                .collect::<Vec<String>>()
                .join("\n")
        ))
    }
}

#[cfg(test)]
mod tests {}
