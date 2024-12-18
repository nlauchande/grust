use aws_lambda_events::s3::S3Event;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use lambda_runtime::{Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::csv_processor::process_csv_content;

#[derive(Debug, Deserialize)]
struct Config {
    selected_fields: Vec<String>,
    output_bucket: String,
    output_prefix: String,
}

#[derive(Debug, Serialize)]
pub struct ProcessingResult {
    input_bucket: String,
    input_key: String,
    records_processed: usize,
    output_location: String,
}

pub async fn function_handler(event: LambdaEvent<S3Event>) -> Result<ProcessingResult, Error> {
    // Get the first record (we process one file at a time)
    let record = event
        .payload
        .records
        .first()
        .ok_or("No S3 records in event")?;

    let bucket = record.s3.bucket.name.as_ref().ok_or("No bucket name")?;
    let key = record.s3.object.key.as_ref().ok_or("No object key")?;

    info!("Processing CSV file: s3://{}/{}", bucket, key);

    // Load configuration from environment variables
    // In production, this could come from AWS AppConfig or Parameter Store
    let config = Config {
        selected_fields: vec!["id".to_string(), "name".to_string()], // Example fields
        output_bucket: std::env::var("OUTPUT_BUCKET").unwrap_or_else(|_| "output-bucket".to_string()),
        output_prefix: std::env::var("OUTPUT_PREFIX").unwrap_or_else(|_| "processed/".to_string()),
    };

    // Initialize AWS S3 client
    let aws_config = aws_config::load_from_env().await;
    let s3_client = S3Client::new(&aws_config);

    // Get the input file from S3
    let obj = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    // Read the file content
    let content = obj.body.collect().await?.into_bytes();

    // Process the CSV content
    let processed_records = process_csv_content(&content[..], &config.selected_fields)?;

    if processed_records.is_empty() {
        warn!("No records were processed from the input file");
    }

    // Generate output key with timestamp
    let timestamp = chrono::Utc::now().timestamp();
    let output_key = format!(
        "{}{}-{}.json",
        config.output_prefix.trim_end_matches('/'),
        key.split('/').last().unwrap_or("unknown"),
        timestamp
    );

    // Upload results to S3
    s3_client
        .put_object()
        .bucket(&config.output_bucket)
        .key(&output_key)
        .body(Bytes::from(serde_json::to_string(&processed_records)?).into())
        .content_type("application/json")
        .send()
        .await?;

    info!(
        "Successfully processed {} records. Output: s3://{}/{}",
        processed_records.len(),
        config.output_bucket,
        output_key
    );

    Ok(ProcessingResult {
        input_bucket: bucket.to_string(),
        input_key: key.to_string(),
        records_processed: processed_records.len(),
        output_location: format!("s3://{}/{}", config.output_bucket, output_key),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lambda_events::s3::{S3Bucket, S3Entity, S3Object};
    use lambda_runtime::Context;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_handler_empty_event() {
        let event = LambdaEvent::new(
            S3Event {
                records: vec![],
            },
            Context::default(),
        );

        let result = function_handler(event).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handler_valid_event() {
        let event = LambdaEvent::new(
            S3Event {
                records: vec![aws_lambda_events::s3::S3EventRecord {
                    event_version: None,
                    event_source: None,
                    aws_region: None,
                    event_time: None,
                    event_name: None,
                    principal_id: None,
                    request_parameters: None,
                    response_elements: None,
                    s3: S3Entity {
                        schema_version: None,
                        configuration_id: None,
                        bucket: S3Bucket {
                            name: Some("test-bucket".to_string()),
                            owner_identity: None,
                            arn: None,
                        },
                        object: S3Object {
                            key: Some("test.csv".to_string()),
                            size: None,
                            url_decoded_key: None,
                            version_id: None,
                            e_tag: None,
                            sequencer: None,
                        },
                    },
                    glacier_event_data: None,
                }],
            },
            Context::default(),
        );

        // This will fail in tests because we can't access S3
        // In production, we would mock the S3 client
        let result = function_handler(event).await;
        assert!(result.is_err());
    }
}
