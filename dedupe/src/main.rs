// Features
// High-performance Rust-based deduplication tool.
// Supports both exact and probabilistic deduplication.
// Integrates with DuckDB for large dataset handling.
// Multi-threaded for faster processing.
// Export in multiple formats (CSV, Excel, JSON, SQL).


mod deduplication;

use clap::{App, Arg};
use crate::deduplication::{read_excel_with_duckdb, save_to_file, exact_match_dedup, probabilistic_match_dedup};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("Rust Deduplication Tool")
        .version("1.0")
        .author("Kabir Dhruw")
        .about("Performs deduplication using exact and probabilistic matching")
        .arg(Arg::new("input")
            .short('i')
            .long("input")
            .value_name("FILE")
            .about("Sets the input Excel file")
            .takes_value(true)
            .required(true))
        .arg(Arg::new("output")
            .short('o')
            .long("output")
            .value_name("FILE")
            .about("Sets the output file path (CSV, Excel, JSON)")
            .takes_value(true)
            .required(true))
        .arg(Arg::new("threshold")
            .short('t')
            .long("threshold")
            .value_name("FLOAT")
            .about("Jaccard similarity threshold for probabilistic deduplication")
            .takes_value(true)
            .default_value("0.8"))
        .get_matches();

    let input_file = matches.value_of("input").unwrap();
    let output_file = matches.value_of("output").unwrap();
    let threshold: f64 = matches.value_of("threshold").unwrap().parse()?;

    println!("Reading data from '{}'", input_file);
    let mut data = read_excel_with_duckdb(input_file)?;

    println!("Performing exact match deduplication...");
    let exact_deduped_data = exact_match_dedup(&mut data);

    println!("Performing probabilistic match deduplication...");
    let probabilistic_deduped_data = probabilistic_match_dedup(&exact_deduped_data, threshold);

    println!("Saving deduplicated data to '{}'", output_file);
    save_to_file(probabilistic_deduped_data, output_file)?;

    println!("Deduplication complete!");
    Ok(())
}

