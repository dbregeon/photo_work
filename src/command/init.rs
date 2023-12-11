use std::{fs, path::PathBuf};

use clap::{arg, ArgMatches, Command};
use eyre::Result;

use crate::{clapext::SubApplication, database::open};

const INIT: &str = "init";

pub(crate) struct Init;

impl SubApplication for Init {
    fn name(&self) -> &'static str {
        INIT
    }

    fn command(&self) -> Command {
        Command::new(self.name())
            .about("Initializes a photo_works repository")
            .arg(arg!(<PATH> "The path of photo_works repository"))
            .arg_required_else_help(true)
    }

    fn handle(&self, sub_matches: &ArgMatches) -> Result<()> {
        let path = sub_matches
            .get_one::<String>("PATH")
            .expect("required")
            .as_str();
        println!("Initializing {}", path);

        Ok(println!("Initialized in {:?}", init(path)?))
    }
}

fn init(parent_path: &str) -> Result<PathBuf> {
    let mut path: PathBuf = [parent_path, ".photo_works"].iter().collect();

    fs::create_dir_all(&path)?;
    path.push("db.db3");
    open(&path)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use crate::{command::init::INIT, SubApplication};

    use super::Init;

    #[test]
    fn command_is_consistent() {
        Init.command().debug_assert();
    }

    #[test]
    fn name_is_init() {
        assert_eq!(INIT, Init.name());
    }

    #[test]
    fn path_is_mandatory() {
        assert_eq!("Initializes a photo_works repository\n\nUsage: init <PATH>\n\nArguments:\n  <PATH>  The path of photo_works repository\n\nOptions:\n  -h, --help  Print help\n",
            Init.command()
                .try_get_matches_from(vec!["init"])
                .map_err(|e| e.to_string())
                .err()
                .unwrap()
        );
    }
}
