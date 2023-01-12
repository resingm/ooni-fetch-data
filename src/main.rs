use std::fs::File;
use std::io::{BufRead, BufReader};

mod data;

const debug_file: &str = "/home/max/workspace/de/maxresing/ooni-fetch-data/data/50rows.jsonl";


fn main() {
    println!("Hello, world!");

    let file = File::open(debug_file).unwrap();
    let buf = BufReader::new(file);

    for (i, l) in buf.lines().enumerate() {
        if i > 1 {
            return
        }

        let l = l.unwrap();
        match data::parse_record(&l) {
            Ok(o) => println!("{}", o.pretty(2)),
            Err(e) => println!("Error: {:?}", e),
        };
    }

}
