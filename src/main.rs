extern crate clap;
extern crate josefine_raft;

use clap::App;
use clap::Arg;

fn main() {
    let matches = App::new("Josefine")
        .version("0.0.1")
        .author("jcm")
        .about("Distributed log in rust.")
        .arg(Arg::with_name("config")
            .long("config")
            .value_name("PATH")
            .required(true)
            .default_value("Config.toml")
            .help("Location of the config file."))
        .get_matches();

    let config_path = matches.value_of("config").unwrap();
}
