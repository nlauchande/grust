use aws_lambda_runtime::Error;
use csv::Reader;
use serde_json::{json, Value};
use std::io::Read;

pub fn process_csv_content<R: Read>(
    reader: R,
    selected_fields: &[String],
) -> Result<Vec<Value>, Error> {
    let mut csv_reader = Reader::from_reader(reader);
    let headers: Vec<String> = csv_reader
        .headers()?
        .iter()
        .map(String::from)
        .collect();

    let field_indices: Vec<usize> = selected_fields
        .iter()
        .filter_map(|field| headers.iter().position(|h| h == field))
        .collect();

    let mut records = Vec::new();

    for result in csv_reader.records() {
        let record = result?;
        let mut field_values = json!({});

        for (field, &index) in selected_fields.iter().zip(&field_indices) {
            if let Some(value) = record.get(index) {
                field_values[field] = json!(value);
            }
        }

        records.push(field_values);
    }

    Ok(records)
}
