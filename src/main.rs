use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};

mod data;

const DEBUG_FILE: &str = "/home/max/workspace/de/maxresing/ooni-fetch-data/data/50rows.jsonl";

fn main() {
    let file = File::open(DEBUG_FILE).unwrap();
    let buf = BufReader::new(file);

    for (i, l) in buf.lines().enumerate() {
        if i > 1 {
            return
        }

        let l = l.unwrap();
        match data::parse_record(&l) {
            // Ok(o) => println!("{}", o.pretty(2)),
            // Err(e) => eprintln!("Error: {:?}", e),
            Ok(m) => log(format!("{:?}", m).as_str()),
            Err(e) => err(&e),
        };
    }

}

fn log(msg: &str) {
    // let mut stdout = io::stdout().lock();
    io::stdout().write(msg.as_bytes()).unwrap();
    io::stdout().write("\n".as_bytes()).unwrap();
}

fn err(msg: &str) {
    io::stderr().write(msg.as_bytes()).unwrap();
    io::stderr().write("\n".as_bytes()).unwrap();
}
