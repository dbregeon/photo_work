use std::collections::HashMap;

use clap::{ArgMatches, Command};
use eyre::Result;

pub(crate) trait SubApplication {
    fn name(&self) -> &'static str;
    fn command(&self) -> Command;
    fn handle(&self, matches: &ArgMatches) -> Result<()>;
}

pub(crate) struct SubCommandHolder {
    sub_commands: HashMap<&'static str, Box<dyn SubApplication>>,
}

impl SubCommandHolder {
    pub(crate) fn new() -> Self {
        Self {
            sub_commands: HashMap::new(),
        }
    }

    pub(crate) fn register(mut self, sub_app: impl SubApplication + 'static) -> Self {
        self.sub_commands.insert(sub_app.name(), Box::new(sub_app));
        self
    }

    pub(crate) fn enrich_command(&self, mut command: Command) -> Command {
        for sub_command in self.sub_commands.values() {
            command = command.subcommand(sub_command.command());
        }
        command
    }

    pub(crate) fn handle(&self, sub_matches: &ArgMatches) -> Result<()> {
        let sub_command = sub_matches.subcommand();
        match sub_command {
            Some((name, sub_matches)) => match self.sub_commands.get(name) {
                Some(command) => command.handle(sub_matches),
                None => unreachable!("Unsupported subcommand `{name}`"),
            },
            None => unreachable!("Missing subcommand."),
        }
    }
}
