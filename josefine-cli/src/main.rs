#[macro_use]
extern crate nom;

use std::io;
use std::str::FromStr;

#[derive(Debug)]
enum Operation {
    Info,
    Get,
}

impl FromStr for  Operation {
    type Err = ();

    fn from_str(s: &str) -> Result<Operation, ()> {
        match s.to_lowercase().as_str() {
            "info" => Ok(Operation::Info),
            "get" => Ok(Operation::Get),
            _ => Err(()),
        }
    }
}

struct Command {
    operation: Operation,
}

named!(get_greeting<&str,&str>,
    tag_s!("hi")
);

named!(get_command<&str, &str>,
    alt!(tag_s!("info") | tag_s!("get"))
);


fn main() {
    let mut line = String::new();
    loop {
        line.clear();
        match io::stdin().read_line(&mut line) {
            Ok(size) => {
                let operation = match get_command(&line) {
                    Ok((remaining, token)) => {
                        Operation::from_str(token).unwrap()
                    },
                    Err(_) => {
                        println!("TODO: Help text.");
                        continue
                    },
                };
                println!("{:?}", operation);
            }
            Err(e) => {}
        }
    }
}

