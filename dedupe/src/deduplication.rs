use calamine::{open_workbook_auto, Reader};
use duckdb::{Connection, Result as DuckDBResult};
use rayon::prelude::*;
use serde_json;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;

// Read Excel with DuckDB
pub fn read_excel_with_duckdb(file_path: &str) -> DuckDBResult<Vec<Vec<String>>> {
    let conn = Connection::open_in_memory()?;
    let mut data = vec![];

    conn.execute(
        "CREATE TABLE input AS SELECT * FROM read_excel_auto(?)",
        &[file_path],
    )?;

    let mut stmt = conn.prepare("SELECT * FROM input")?;
    let rows = stmt.query_map([], |row| {
        Ok((0..row.column_count())
            .map(|i| row.get_unwrap::<_, String>(i))
            .collect::<Vec<_>>())
    })?;

    for row in rows {
        data.push(row?);
    }

    Ok(data)
}

// Save to various formats
pub fn save_to_file(data: Vec<Vec<String>>, output_file: &str) -> std::io::Result<()> {
    if output_file.ends_with(".csv") {
        let mut wtr = csv::Writer::from_path(output_file)?;
        for row in data {
            wtr.write_record(row)?;
        }
        wtr.flush()?;
    } else if output_file.ends_with(".json") {
        let json_data = serde_json::to_string_pretty(&data)?;
        let mut file = File::create(output_file)?;
        file.write_all(json_data.as_bytes())?;
    } else if output_file.ends_with(".xlsx") {
        // Write as Excel
        unimplemented!("Excel writing is not yet supported.");
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Unsupported file format",
        ));
    }
    Ok(())
}

// Exact match deduplication
pub fn exact_match_dedup(data: &mut Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut seen: HashSet<Vec<String>> = HashSet::new();
    data.drain(..).filter(|row| seen.insert(row.clone())).collect()
}

// Probabilistic match deduplication
pub fn probabilistic_match_dedup(data: &Vec<Vec<String>>, threshold: f64) -> Vec<Vec<String>> {
    let mut unique_data: Vec<Vec<String>> = Vec::new();

    data.par_iter().for_each(|row| {
        if !unique_data.par_iter().any(|existing_row| {
            let jaccard_index = jaccard_similarity(row, existing_row);
            jaccard_index >= threshold
        }) {
            unique_data.push(row.clone());
        }
    });

    unique_data
}

// Jaccard similarity calculation
fn jaccard_similarity(a: &Vec<String>, b: &Vec<String>) -> f64 {
    let set_a: HashSet<&String> = a.iter().collect();
    let set_b: HashSet<&String> = b.iter().collect();

    let intersection = set_a.intersection(&set_b).count() as f64;
    let union = set_a.union(&set_b).count() as f64;

    intersection / union
}
