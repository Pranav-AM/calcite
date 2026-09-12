// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Converts Calcite-generated Arrow fixtures to Parquet without a second data generator.

use std::fs::File;

use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::parquet::arrow::ArrowWriter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: arrow_to_parquet <input.arrow> <output.parquet>".into());
    }
    let reader = StreamReader::try_new(File::open(&args[1])?, None)?;
    let mut writer = ArrowWriter::try_new(File::create(&args[2])?, reader.schema(), None)?;
    for batch in reader {
        writer.write(&batch?)?;
    }
    writer.close()?;
    Ok(())
}
