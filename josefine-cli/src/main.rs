#[macro_use]
extern crate nom;

use std::io;
use std::str::FromStr;
use clap::App;
use clap::Arg;
use std::str;
use std::net::SocketAddr;

#[derive(Debug)]
enum Operation {
    Info,
    Get,
    Add(SocketAddr),
    None,
}

impl FromStr for Operation {
    type Err = ();

    fn from_str(s: &str) -> Result<Operation, ()> {
        match s.to_lowercase().as_str() {
            "info" => Ok(Operation::Info),
            "get" => Ok(Operation::Get),
            _ => Err(()),
        }
    }
}

named!(not_space, is_not!(" \t\r\n"));

named!(get_word<&[u8], &str>,
    map_res!(not_space, str::from_utf8));

named!(get_socket_addr<&[u8], SocketAddr>,
    map_res!(get_word, FromStr::from_str));

named!(add_op<&[u8], Operation>, do_parse!(
    tag_no_case!("info") >>
    take_till!(nom::is_digit) >>
    socket_addr: get_socket_addr >>
    (Operation::Add(socket_addr))
));


fn main() {
    let matches = App::new("josefine-cli")
        .version("0.0.1")
        .author("jcm")
        .about("Distributed log in rust.")
        .arg(Arg::with_name("port")
            .long("port")
            .value_name("PORT")
            .required(true)
            .default_value("8081")
            .help("Port to connect to."))
        .get_matches();

    let mut line = String::new();
    loop {
        line.clear();
        match io::stdin().read_line(&mut line) {
            Ok(size) => {
                let operation = add_op(&line.as_bytes());
                println!("{:?}", operation);
            }
            Err(e) => {}
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let (_, operation) = add_op(b"info 127.0.0.1:8080\n").unwrap();
        match operation {
            Operation::Add(addr) => {}
            _ => panic!()
        }
    }
}
