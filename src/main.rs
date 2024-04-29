use std::ffi::OsString;

use clap::Command;
use clapext::{SubApplication, SubCommandHolder};
use command::{catalog, check, import, init};
use eyre::Result;

mod clapext;
mod command;
mod database;

struct PhotoWorks {
    sub_commands: SubCommandHolder,
}

impl PhotoWorks {
    fn new() -> Self {
        PhotoWorks {
            sub_commands: SubCommandHolder::new(),
        }
    }

    fn command(&self) -> Command {
        let command = Command::new("photo_works")
            .about("A photo management CLI")
            .subcommand_required(true)
            .arg_required_else_help(true)
            .allow_external_subcommands(true);
        self.sub_commands.enrich_command(command)
    }

    fn register(mut self, sub_command: impl SubApplication + 'static) -> Self {
        self.sub_commands = self.sub_commands.register(sub_command);
        self
    }

    fn run<I, T>(self, itr: I) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        self.sub_commands
            .handle(&self.command().get_matches_from(itr))
    }
}

fn app() -> PhotoWorks {
    PhotoWorks::new()
        .register(init::Init)
        .register(catalog::Catalog)
        .register(import::Import)
        .register(check::Check)
}

fn main() -> Result<()> {
    env_logger::init();

    app().run(std::env::args_os())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use clap::{ArgMatches, Command};
    use eyre::Result;

    use crate::{app, clapext::SubApplication, PhotoWorks};

    #[test]
    fn register_add_a_sub_application_command() {
        let (_, sub_app) = given_a_sub_app();
        let app = PhotoWorks::new().register(sub_app);
        app.command()
            .get_matches_from(vec!["photo_works", "test"])
            .subcommand_matches("test")
            .unwrap();
    }

    #[test]
    fn run_invokes_the_subcommand_handle() {
        let (invoked, sub_app) = given_a_sub_app();
        let app = PhotoWorks::new().register(sub_app);

        app.run(vec!["photo_works", "test"]).unwrap();

        assert!(invoked.load(Ordering::Relaxed))
    }

    #[test]
    #[should_panic(expected = "Unsupported subcommand `other_command`")]
    fn panics_when_subcommand_cannot_be_found() {
        let (_, sub_app) = given_a_sub_app();
        let app = PhotoWorks::new().register(sub_app);
        app.run(vec!["photo_works", "other_command"]).unwrap();
    }

    #[test]
    fn command_is_consistent() {
        let app = app();

        app.command().debug_assert();
    }

    fn given_a_sub_app() -> (Arc<AtomicBool>, TestSubApp) {
        let invoked_flag = Arc::new(AtomicBool::new(false));
        (
            invoked_flag.clone(),
            TestSubApp {
                invoked: invoked_flag,
            },
        )
    }

    struct TestSubApp {
        invoked: Arc<AtomicBool>,
    }

    impl SubApplication for TestSubApp {
        fn name(&self) -> &'static str {
            "test"
        }

        fn command(&self) -> Command {
            Command::new("test")
        }

        fn handle(&self, _: &ArgMatches) -> Result<()> {
            self.invoked
                .store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }
}
