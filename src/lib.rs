mod csv_processor;

use aws_lambda_runtime::{run, service_fn, Error, LambdaEvent};
use aws_lambda_events::s3::S3Event;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tracing::{info, warn};

use csv_processor::process_csv_content;

#[derive(Debug, Deserialize)]
struct Config {
    selected_fields: Vec<String>,
    output_endpoint: String,
}

#[derive(Debug, Serialize)]
struct ProcessedRecord {
    fields: serde_json::Value,
}

async fn process_csv(
    bucket: &str,
    key: &str,
    config: &Config,
) -> Result<Vec<ProcessedRecord>, Error> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);
    
    let obj = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
        
    let content = obj.body.collect().await?.into_bytes();
    
    let processed_values = process_csv_content(content.as_slice(), &config.selected_fields)?;
    
    Ok(processed_values
        .into_iter()
        .map(|fields| ProcessedRecord { fields })
        .collect())
}

async fn write_output(
    records: Vec<ProcessedRecord>,
    endpoint: &str,
) -> Result<(), Error> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);
    
    // Parse S3 URL (e.g., s3://bucket/key)
    let endpoint = endpoint.trim_start_matches("s3://");
    let mut parts = endpoint.splitn(2, '/');
    let bucket = parts.next().ok_or("Invalid S3 URL: no bucket")?;
    let key_prefix = parts.next().ok_or("Invalid S3 URL: no key prefix")?;
    
    // Convert records to JSON and upload
    let json_content = serde_json::to_string(&records)?;
    let output_key = format!("{}{}.json", key_prefix.trim_end_matches('/'), chrono::Utc::now().timestamp());
    
    client
        .put_object()
        .bucket(bucket)
        .key(output_key)
        .body(json_content.into_bytes().into())
        .content_type("application/json")
        .send()
        .await?;
    
    Ok(())
}

async fn function_handler(event: LambdaEvent<S3Event>) -> Result<(), Error> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let (event, _context) = event.into_parts();
    
    // Get configuration from environment or event
    // TODO: Implement proper config loading
    let config = Config {
        selected_fields: vec!["id".to_string(), "name".to_string()],
        output_endpoint: "s3://output-bucket/processed/".to_string(),
    };

    for record in event.records {
        let bucket = record.s3.bucket.name.unwrap_or_default();
        let key = record.s3.object.key.unwrap_or_default();
        
        info!("Processing file {}/{}", bucket, key);
        
        match process_csv(&bucket, &key, &config).await {
            Ok(records) => {
                info!("Successfully processed {} records", records.len());
                if let Err(e) = write_output(records, &config.output_endpoint).await {
                    warn!("Failed to write output: {}", e);
                }
            }
            Err(e) => warn!("Failed to process CSV: {}", e),
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    run(service_fn(function_handler)).await
}
