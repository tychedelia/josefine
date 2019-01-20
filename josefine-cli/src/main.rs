#[macro_use]
extern crate nom;

use std::io;
use std::str::FromStr;
use clap::App;
use clap::Arg;
use std::str;
use std::net::SocketAddr;
use nom::types::CompleteByteSlice;
use std::io::Write;
use std::io::Read;
use std::net::TcpStream;
use std::net::IpAddr;
use rustyline::error::ReadlineError;
use rustyline::Editor;


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

named!(add_op<CompleteByteSlice, Operation>, do_parse!(
    tag_no_case!("add") >>
    take_till!(nom::is_digit) >>
    addr: take_till1!(nom::is_space) >>
    (Operation::Add(str::from_utf8(*addr).unwrap().parse().unwrap()))
));

named!(info_op<CompleteByteSlice, Operation>, ws!(do_parse!(
    tag_no_case!("info") >>
    (Operation::Info)
)));

named!(get_op<CompleteByteSlice, Operation>, alt!(info_op | add_op));


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

    let address: IpAddr = "127.0.0.1".parse().unwrap();
    let port: u16 = matches.value_of("port").unwrap().parse().unwrap();
    let socket_addr = SocketAddr::new(address, port);

    let connection = TcpStream::connect(socket_addr).expect("Couldn't connect!");

    println!("Connected!");

    let mut rl = Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_ref());
                println!("Line: {:?}", get_op(CompleteByteSlice(&line.as_bytes())));
            },
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break
            },
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
    rl.save_history("history.txt").unwrap();
}

fn print_prompt() {
    print!(">> ");
    io::stdout().flush().unwrap();
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let (_, operation) = add_op(b"aDd 127.0.0.1:8080\n").unwrap();
        match operation {
            Operation::Add(addr) => {}
            _ => panic!()
        }
    }
}
