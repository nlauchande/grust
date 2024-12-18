use csv::Reader;
use serde_json::{json, Value};
use std::io::Read;
use tracing::{info, warn};

pub fn process_csv_content<R: Read>(
    reader: R,
    selected_fields: &[String],
) -> Result<Vec<Value>, Box<dyn std::error::Error + Send + Sync>> {
    let mut csv_reader = Reader::from_reader(reader);
    let headers: Vec<String> = csv_reader
        .headers()?
        .iter()
        .map(String::from)
        .collect();

    info!("CSV headers: {:?}", headers);

    // Find indices of selected fields
    let field_indices: Vec<usize> = selected_fields
        .iter()
        .filter_map(|field| {
            let pos = headers.iter().position(|h| h == field);
            if pos.is_none() {
                warn!("Field '{}' not found in CSV headers", field);
            }
            pos
        })
        .collect();

    if field_indices.is_empty() {
        warn!("No selected fields were found in the CSV headers");
        return Ok(Vec::new());
    }

    info!("Processing CSV with selected fields: {:?}", selected_fields);

    let mut records = Vec::new();
    for (row_idx, result) in csv_reader.records().enumerate() {
        let record = result?;
        let mut field_values = json!({});

        for (field, &index) in selected_fields.iter().zip(&field_indices) {
            if let Some(value) = record.get(index) {
                field_values[field] = json!(value.trim());
            }
        }

        if row_idx % 1000 == 0 {
            info!("Processed {} records", row_idx);
        }

        records.push(field_values);
    }

    info!("Completed processing {} records", records.len());
    Ok(records)
}