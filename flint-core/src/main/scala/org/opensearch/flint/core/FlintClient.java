/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import java.util.List;

import org.opensearch.client.RestHighLevelClient;
import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.flint.core.storage.FlintReader;
import org.opensearch.flint.core.storage.FlintWriter;

/**
 * Flint index client that provides API for metadata and data operations
 * on a Flint index regardless of concrete storage.
 */
public interface FlintClient {

  /**
   * Create a Flint index with the metadata given.
   *
   * @param indexName index name
   * @param metadata  index metadata
   */
  void createIndex(String indexName, FlintMetadata metadata);

  /**
   * Create an alias name for the given index
   * @param indexName
   * @param aliasName
   */
  void alias(String indexName, String aliasName, FlintMetadata metadata);

  /**
   * Does Flint index with the given name exist
   *
   * @param indexName index name
   * @return true if the index exists, otherwise false
   */
  boolean exists(String indexName);

  /**
   * Retrieve all metadata for Flint index whose name matches the given pattern.
   *
   * @param indexNamePattern index name pattern
   * @return all matched index metadata
   */
  List<FlintMetadata> getAllIndexMetadata(String indexNamePattern);

  /**
   * Retrieve metadata in a Flint index.
   *
   * @param indexName index name
   * @return index metadata
   */
  FlintMetadata getIndexMetadata(String indexName);

  /**
   * Delete a Flint index.
   *
   * @param indexName index name
   */
  void deleteIndex(String indexName);

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query DSL query. DSL query is null means match_all
   * @return {@link FlintReader}.
   */
  FlintReader createReader(String indexName, String query);

  /**
   * Create {@link FlintWriter}.
   *
   * @param indexName - index name
   * @return {@link FlintWriter}
   */
  FlintWriter createWriter(String indexName);

  /**
   * Create {@link RestHighLevelClient}.
   * @return {@link RestHighLevelClient}
   */
  public RestHighLevelClient createClient();
}
