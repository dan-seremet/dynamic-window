use std::{path::Path, io::BufReader};
use std::io::{Read, BufRead};
use std::fs;

use crate::viewingperiod::{ViewingPeriod, Status};

/// Holds the names of the columns in a TSV or CSV file
type Header<'a> = Vec<&'a str>;

fn separator(path: impl AsRef<Path>) -> Option<char> {
    match path.as_ref().extension() {
        None => None,
        Some(os_str) => match os_str.to_str() {
            None => None,
            Some("csv") => Some(','),
            Some("tsv") => Some('\t'),
            Some(_) => None
        }
    }
}

fn read(path: impl AsRef<Path>) -> Vec<ViewingPeriod> {
    let sep = separator(&path)
        .expect("unsupported file extension");

    let file = fs::File::open(&path)
        .expect("failed to open file");
    let reader = BufReader::new(file);
    let mut lines_iter = reader.lines();
    let header_line = lines_iter.next()
        .expect("expected table to have at least header")
        .expect("failed to read header from file");
    let header: Header = header_line.split(sep).collect();

    return lines_iter
        .filter_map(|line|
            line.map_err(|err| println!("failed to read period line: {}", err)).ok())
        .map(|line| line_to_period(&line, &header))
        .collect();
}

fn line_to_period(line: &str, header: &Header) -> ViewingPeriod {
    return ViewingPeriod::default();
}
