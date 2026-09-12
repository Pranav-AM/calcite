/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.adapter.fqp.cost.DataFusionColumnStatistics;
import org.apache.calcite.adapter.fqp.cost.DataFusionCostParameters;
import org.apache.calcite.adapter.fqp.cost.DataFusionStatisticsSnapshot;
import org.apache.calcite.adapter.fqp.cost.DataFusionTableStatistics;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Loads query-independent FQP deployment metadata from JSON. */
public final class FqpDeploymentConfigLoader {
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .disable(JsonParser.Feature.AUTO_CLOSE_SOURCE)
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
      .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
      .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  private FqpDeploymentConfigLoader() {
  }

  public static FqpPlanningConfig load(Path path) throws IOException {
    return loadDeployment(path).planningConfig();
  }

  public static FqpPlanningConfig load(InputStream input) throws IOException {
    return loadDeployment(input).planningConfig();
  }

  /** Loads planning metadata, statistics and cost coefficients without worker calls. */
  public static FqpDeployment loadDeployment(Path path) {
    Objects.requireNonNull(path, "path");
    try (InputStream input = Files.newInputStream(path)) {
      return loadDeployment(input);
    } catch (IOException | FqpDeploymentException e) {
      throw new FqpDeploymentException("invalid deployment " + path + ": "
          + e.getMessage(), e);
    }
  }

  /** Leaves the caller's stream open. */
  public static FqpDeployment loadDeployment(InputStream input) {
    Objects.requireNonNull(input, "input");
    try {
      return Objects.requireNonNull(MAPPER.readValue(input, DeploymentDocument.class),
          "deployment document").toDeployment();
    } catch (IOException | RuntimeException e) {
      throw new FqpDeploymentException("invalid deployment: " + e.getMessage(), e);
    }
  }

  /** JSON deployment root. */
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  @JsonIgnoreProperties(ignoreUnknown = false)
  private static final class DeploymentDocument {
    @JsonProperty(required = true)
    private String coordinatorSourceId;
    @JsonProperty(required = true)
    private List<DestinationDocument> destinations;
    @JsonProperty(required = true)
    private List<PlacementDocument> placements;
    private double movementCostFactor;
    private boolean allowRemoteRoot = true;
    private long remoteCostTimeoutMillis = 30_000L;
    private Map<String, SnapshotDocument> statistics = Collections.emptyMap();
    private Map<String, CostDocument> costParameters = Collections.emptyMap();

    private FqpDeployment toDeployment() {
      final FqpPlanningConfig config = toPlanningConfig();
      for (PlacementDocument placement : placements) {
        requireDestination(config, placement.sourceId);
        placement.visibleNames.keySet().forEach(source -> requireDestination(config, source));
      }
      final Map<String, DataFusionStatisticsSnapshot> snapshots = new LinkedHashMap<>();
      Objects.requireNonNull(statistics, "statistics").forEach((source, snapshot) -> {
        requireDestination(config, source);
        snapshots.put(source, Objects.requireNonNull(snapshot, "statistics for " + source)
            .toSnapshot(source, config));
      });
      final Map<String, DataFusionCostParameters> costs = new LinkedHashMap<>();
      Objects.requireNonNull(costParameters, "costParameters").forEach((source, cost) -> {
        requireDestination(config, source);
        costs.put(source, Objects.requireNonNull(cost, "costParameters for " + source)
            .toParameters());
      });
      return new FqpDeployment(config, snapshots, costs);
    }

    private FqpPlanningConfig toPlanningConfig() {
      if (remoteCostTimeoutMillis <= 0L) {
        throw new IllegalArgumentException("remoteCostTimeoutMillis must be positive");
      }
      nonNegative(movementCostFactor, "movementCostFactor");
      return FqpPlanningConfig.of(coordinatorSourceId,
          map(destinations, DestinationDocument::toDestination, "destinations"),
          map(placements, PlacementDocument::toPlacement, "placements"),
          movementCostFactor, allowRemoteRoot,
          Duration.ofMillis(remoteCostTimeoutMillis));
    }
  }

  /** JSON worker infrastructure. */
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  @JsonIgnoreProperties(ignoreUnknown = false)
  private static final class DestinationDocument {
    @JsonProperty(required = true)
    private String sourceId;
    private String dialect = "CALCITE";
    @JsonProperty(required = true)
    private Set<FqpFragmentPayload.Format> payloadFormats;
    private String executionEndpoint;
    private String costEndpoint;
    private String flightEndpoint;

    private FqpDestination toDestination() {
      final FqpDestinationCapabilities capabilities = FqpDestinationCapabilities.of(
          copyFormats(payloadFormats), uri(executionEndpoint, false), uri(costEndpoint, false),
          uri(flightEndpoint, true));
      return new FqpDestination(sourceId, dialect(dialect), capabilities);
    }
  }

  /** JSON table ownership and physical names. */
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  @JsonIgnoreProperties(ignoreUnknown = false)
  private static final class PlacementDocument {
    @JsonProperty(required = true)
    private List<String> logicalName;
    @JsonProperty(required = true)
    private String sourceId;
    @JsonProperty(required = true)
    private Map<String, List<String>> visibleNames;

    private FqpTablePlacement toPlacement() {
      final Map<String, List<String>> names = new LinkedHashMap<>();
      Objects.requireNonNull(visibleNames, "visibleNames").forEach(names::put);
      return new FqpTablePlacement(logicalName, sourceId, names);
    }
  }

  /** JSON statistics for one worker. */
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  private static final class SnapshotDocument {
    private String version;
    private String loadedAt;
    private List<TableDocument> tables;

    private DataFusionStatisticsSnapshot toSnapshot(String source, FqpPlanningConfig config) {
      final Map<List<String>, DataFusionTableStatistics> values = new LinkedHashMap<>();
      for (TableDocument table : Objects.requireNonNull(tables, "statistics tables")) {
        Objects.requireNonNull(table, "statistics table for " + source);
        final List<String> name = FqpPlanningConfig.copyName(table.logicalName);
        final FqpTablePlacement placement = config.placement(name).orElseThrow(() ->
            new IllegalArgumentException("statistics reference unplaced table " + name));
        if (!source.equals(placement.sourceId())) {
          throw new IllegalArgumentException("statistics for " + name
              + " must belong to " + placement.sourceId());
        }
        if (values.put(name, table.toStatistics()) != null) {
          throw new IllegalArgumentException("duplicate statistics table " + name);
        }
      }
      return new DataFusionStatisticsSnapshot(source, version,
          Instant.parse(Objects.requireNonNull(loadedAt, "statistics loadedAt")), values);
    }
  }

  /** JSON table statistics; SQL types remain in Calcite. */
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  private static final class TableDocument {
    private List<String> logicalName;
    private Double rowCount;
    private Integer averageRowWidth;
    private Map<String, ColumnDocument> columns = Collections.emptyMap();

    private DataFusionTableStatistics toStatistics() {
      final Map<String, DataFusionColumnStatistics> values = new LinkedHashMap<>();
      Objects.requireNonNull(columns, "columns").forEach((name, column) -> {
        final String key = FqpDestination.requireNonBlank(name, "column name")
            .toUpperCase(Locale.ROOT);
        if (values.put(key, Objects.requireNonNull(column, "statistics column " + name)
            .toStatistics()) != null) {
          throw new IllegalArgumentException("duplicate statistics column " + name);
        }
      });
      return new DataFusionTableStatistics(nonNegative(
          Objects.requireNonNull(rowCount, "rowCount"), "rowCount"),
          Objects.requireNonNull(averageRowWidth, "averageRowWidth"), values);
    }
  }

  /** JSON column cardinality statistics. */
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  private static final class ColumnDocument {
    private Double distinctCount;
    private Double nullCount;

    private DataFusionColumnStatistics toStatistics() {
      return DataFusionColumnStatistics.of(nonNegative(
          Objects.requireNonNull(distinctCount, "distinctCount"), "distinctCount"),
          nonNegative(Objects.requireNonNull(nullCount, "nullCount"), "nullCount"));
    }
  }

  /** JSON cost coefficient overrides. */
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  private static final class CostDocument {
    private double scanCpuPerRow = 0.01D;
    private double expressionCpuPerRow = 0.02D;
    private double joinCpuPerRow = 0.05D;
    private double aggregateCpuPerRow = 0.03D;
    private double sortCpuPerComparison = 0.01D;
    private double ioPerByte = 0.0001D;

    private DataFusionCostParameters toParameters() {
      return new DataFusionCostParameters(scanCpuPerRow, expressionCpuPerRow,
          joinCpuPerRow, aggregateCpuPerRow, sortCpuPerComparison, ioPerByte);
    }
  }

  private static void requireDestination(FqpPlanningConfig config, String source) {
    if (!config.destination(source).isPresent()) {
      throw new IllegalArgumentException("unknown destination " + source);
    }
  }

  private static double nonNegative(double value, String name) {
    if (!Double.isFinite(value) || value < 0D) {
      throw new IllegalArgumentException(name + " must be finite and non-negative");
    }
    return value;
  }

  /** Maps JSON entries to immutable deployment values.
   *
   * @param <T> JSON entry type
   * @param <R> Immutable value type
   */
  private interface Mapper<T, R> {
    R map(T value);
  }

  private static <T, R> Collection<R> map(List<T> values, Mapper<T, R> mapper,
      String name) {
    Objects.requireNonNull(values, name);
    if (values.isEmpty()) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    final List<R> result = new ArrayList<>();
    for (T value : values) {
      result.add(mapper.map(Objects.requireNonNull(value, name + " entry")));
    }
    return result;
  }

  private static EnumSet<FqpFragmentPayload.Format> copyFormats(
      Set<FqpFragmentPayload.Format> formats) {
    Objects.requireNonNull(formats, "payloadFormats");
    if (formats.isEmpty()) {
      throw new IllegalArgumentException("payloadFormats must not be empty");
    }
    return EnumSet.copyOf(formats);
  }

  private static SqlDialect dialect(String name) {
    final String normalized = FqpDestination.requireNonBlank(name, "dialect")
        .toUpperCase(Locale.ROOT);
    switch (normalized) {
    case "CALCITE":
    case "ANSI":
      return CalciteSqlDialect.DEFAULT;
    case "DUCKDB":
      return DuckDBSqlDialect.DEFAULT;
    default:
      throw new IllegalArgumentException("unsupported SQL dialect: " + name);
    }
  }

  private static URI uri(String value, boolean flight) {
    if (value == null) {
      return null;
    }
    final URI uri = URI.create(FqpDestination.requireNonBlank(value, "endpoint"));
    final String scheme = uri.getScheme();
    final boolean supported = flight
        ? "grpc".equalsIgnoreCase(scheme) || "grpc+tls".equalsIgnoreCase(scheme)
        : "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
    if (!supported || uri.getHost() == null || uri.getUserInfo() != null
        || uri.getFragment() != null || uri.getPort() > 65535 || uri.getPort() == 0
        || flight && (uri.getPort() < 1 || uri.getQuery() != null
            || !uri.getPath().isEmpty())) {
      throw new IllegalArgumentException("invalid " + (flight ? "Flight" : "HTTP")
          + " endpoint: " + value);
    }
    return uri;
  }
}
