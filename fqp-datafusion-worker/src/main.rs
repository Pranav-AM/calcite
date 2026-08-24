// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone)]
struct WorkerState {
    source_id: String,
    context: SessionContext,
}

#[derive(Debug, Deserialize)]
struct Config {
    source_id: String,
    bind: SocketAddr,
    #[serde(default)]
    tables: Vec<TableConfig>,
}

#[derive(Debug, Deserialize)]
struct TableConfig {
    name: String,
    path: String,
    format: TableFormat,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TableFormat {
    Csv,
    Parquet,
}

#[derive(Debug, Serialize)]
struct Health {
    source_id: String,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct Estimate {
    startup_cost: f64,
    total_cost: f64,
    row_count: f64,
    row_width: i32,
}

#[derive(Debug, Error)]
enum WorkerError {
    #[error("invalid Substrait plan: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("Arrow IPC error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error("the request body is empty")]
    EmptyBody,
    #[error("the fragment returned no record batches")]
    EmptyResult,
}

impl IntoResponse for WorkerError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, self.to_string()).into_response()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).ok_or("usage: fqp-datafusion-worker <config.toml>")?;
    let config: Config = toml::from_str(&std::fs::read_to_string(path)?)?;
    let context = SessionContext::new();
    for table in config.tables {
        match table.format {
            TableFormat::Csv => context.register_csv(&table.name, &table.path,
                CsvReadOptions::new()).await?,
            TableFormat::Parquet => context.register_parquet(&table.name, &table.path,
                ParquetReadOptions::default()).await?,
        }
    }
    let state = Arc::new(WorkerState { source_id: config.source_id, context });
    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/execute", post(execute))
        .route("/v1/cost", post(cost))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<Arc<WorkerState>>) -> Json<Health> {
    Json(Health { source_id: state.source_id.clone(), status: "ok" })
}

async fn execute(State(state): State<Arc<WorkerState>>, body: Bytes)
    -> Result<Response, WorkerError> {
    let dataframe = data_frame(&state.context, body).await?;
    let batches = dataframe.collect().await?;
    let schema = batches.first().ok_or(WorkerError::EmptyResult)?.schema();
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &schema)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
    }
    let mut response = bytes.into_response();
    response.headers_mut().insert(header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"));
    Ok(response)
}

async fn cost(State(state): State<Arc<WorkerState>>, body: Bytes)
    -> Result<Json<Estimate>, WorkerError> {
    let dataframe = data_frame(&state.context, body).await?;
    let width: i32 = dataframe.schema().fields().iter().map(|field| width_of(field.data_type()))
        .sum();
    let fields = dataframe.schema().fields().len() as f64;
    // DataFusion does not guarantee cardinality statistics for every provider.
    // Keep these values normalized and conservative until provider statistics
    // and cross-engine calibration are introduced.
    Ok(Json(Estimate { startup_cost: 1.0, total_cost: 1.0 + fields,
        row_count: 1000.0, row_width: width.max(1) }))
}

async fn data_frame(context: &SessionContext, body: Bytes)
    -> Result<datafusion::dataframe::DataFrame, WorkerError> {
    if body.is_empty() {
        return Err(WorkerError::EmptyBody);
    }
    let plan = Plan::decode(body.as_ref())?;
    let logical = from_substrait_plan(&context.state(), &plan).await?;
    Ok(context.execute_logical_plan(logical).await?)
}

fn width_of(data_type: &DataType) -> i32 {
    match data_type {
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 => 2,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Date64 => 8,
        _ => 32,
    }
}

#[cfg(test)]
mod tests {
    use super::width_of;
    use datafusion::arrow::datatypes::DataType;

    #[test]
    fn normalizes_type_widths_for_costing() {
        assert_eq!(4, width_of(&DataType::Int32));
        assert_eq!(32, width_of(&DataType::Utf8));
    }
}
