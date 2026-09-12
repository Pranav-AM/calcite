// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct ExchangeFlightService {
    context: SessionContext,
}

impl ExchangeFlightService {
    pub fn new(context: SessionContext) -> Self {
        Self { context }
    }
}

#[tonic::async_trait]
impl FlightService for ExchangeFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut input = request.into_inner();
        let first = input
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty Flight stream"))?;
        let descriptor = first
            .flight_descriptor
            .clone()
            .ok_or_else(|| Status::invalid_argument("missing Flight descriptor"))?;
        if descriptor.r#type != arrow_flight::flight_descriptor::DescriptorType::Path as i32
            || descriptor.path.len() != 1
        {
            return Err(Status::invalid_argument(
                "exchange descriptor must be a single table-name path",
            ));
        }
        let table_name = &descriptor.path[0];
        if !valid_table_name(table_name) {
            return Err(Status::invalid_argument("invalid exchange table name"));
        }
        let flight_data = stream::once(async { Ok(first) })
            .chain(input.map_err(arrow_flight::error::FlightError::from));
        let mut batches = FlightRecordBatchStream::new_from_flight_data(flight_data);
        let mut values = Vec::new();
        while let Some(batch) = batches.next().await {
            values.push(batch.map_err(Status::from)?);
        }
        let schema = batches
            .schema()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("Flight stream has no Arrow schema"))?;
        let table = MemTable::try_new(schema, vec![values])
            .map_err(|error| Status::internal(error.to_string()))?;
        self.context
            .register_table(table_name.as_str(), Arc::new(table))
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(
            stream::once(async { Ok(PutResult::default()) }).boxed(),
        ))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

pub async fn serve(
    listener: TcpListener,
    context: SessionContext,
) -> Result<(), tonic::transport::Error> {
    Server::builder()
        .add_service(FlightServiceServer::new(ExchangeFlightService::new(
            context,
        )))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
}

fn valid_table_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::valid_table_name;

    #[test]
    fn validates_temporary_table_identifiers() {
        assert!(valid_table_name("fqp_exchange_1"));
        for name in ["", "a.b", "a/b", "has space", "1first"] {
            assert!(!valid_table_name(name), "{name}");
        }
    }
}
