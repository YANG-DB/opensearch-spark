/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static java.lang.String.format;
import static org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.FlintVersion;
import org.opensearch.flint.core.auth.AWSRequestSigningApacheInterceptor;
import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import scala.Option;

/**
 * Flint client implementation for OpenSearch storage.
 */
public class FlintOpenSearchClient implements FlintClient {

    /**
     * {@link NamedXContentRegistry} from {@link SearchModule} used for construct {@link QueryBuilder} from DSL query string.
     */
    private final static NamedXContentRegistry
            xContentRegistry =
            new NamedXContentRegistry(new SearchModule(Settings.builder().build(),
                    new ArrayList<>()).getNamedXContents());

    private final FlintOptions options;

    public FlintOpenSearchClient(FlintOptions options) {
        this.options = options;
    }

    @Override
    public void alias(String indexName, String aliasName, FlintMetadata metadata) {
        String osIndexName = toLowercase(indexName);
        String osAliasName = toLowercase(aliasName);
        try (RestHighLevelClient client = createClient()) {
            boolean exists = client.indices().exists(new GetIndexRequest(osIndexName), RequestOptions.DEFAULT);
            if (!exists) {
                // create index including the alias name with is the flint convention name
                createIndex(osIndexName, metadata);
            }
            // Adding the alias to the existing index
            IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
            IndicesAliasesRequest.AliasActions aliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(osIndexName)
                            .alias(osAliasName);
            aliasesRequest.addAliasAction(aliasAction);
            // Executing the updateAliases request
            AcknowledgedResponse response = client.indices().updateAliases(aliasesRequest, RequestOptions.DEFAULT);
            if (!response.isAcknowledged()) {
                throw new IllegalStateException(String.format("Failed to acknowledge Alias %s for index %s", aliasName, indexName));
            }
        } catch (
                Exception e) {
            throw new IllegalStateException(format("Failed to create Alias %s for index %s ", aliasName, indexName), e);
        }
    }

    @Override
    public void createIndex(String indexName, FlintMetadata metadata) {
        String osIndexName = toLowercase(indexName);
        try (RestHighLevelClient client = createClient()) {
            CreateIndexRequest request = new CreateIndexRequest(osIndexName);
            boolean includeMappingProperties = !metadata.targetName().nonEmpty();
            request.mapping(metadata.getContent(includeMappingProperties), XContentType.JSON);
            Option<String> settings = metadata.indexSettings();
            if (settings.isDefined()) {
                request.settings(settings.get(), XContentType.JSON);
            }
            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            if (!response.isAcknowledged()) {
                throw new IllegalStateException(String.format("Failed to acknowledge create index %s", indexName));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create Flint index " + osIndexName, e);
        }
    }

    @Override
    public boolean exists(String indexName) {
        String osIndexName = toLowercase(indexName);
        try (RestHighLevelClient client = createClient()) {
            return client.indices().exists(new GetIndexRequest(osIndexName), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to check if Flint index exists " + osIndexName, e);
        }
    }

    @Override
    public List<FlintMetadata> getAllIndexMetadata(String indexNamePattern) {
        String osIndexNamePattern = toLowercase(indexNamePattern);
        try (RestHighLevelClient client = createClient()) {
            GetIndexRequest request = new GetIndexRequest(osIndexNamePattern);
            GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);

            return Arrays.stream(response.getIndices())
                    .map(index -> FlintMetadata.apply(
                            response.getMappings().get(index).source().toString(),
                            response.getSettings().get(index).toString()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to get Flint index metadata for " + osIndexNamePattern, e);
        }
    }

    @Override
    public FlintMetadata getIndexMetadata(String indexName) {
        String osIndexName = toLowercase(indexName);
        try (RestHighLevelClient client = createClient()) {
            GetIndexRequest request = new GetIndexRequest(osIndexName);
            GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
            if (response.getMappings().containsKey(osIndexName)) {
                MappingMetadata mapping = response.getMappings().get(osIndexName);
                Settings settings = response.getSettings().get(osIndexName);
                return FlintMetadata.apply(mapping.source().string(), settings.toString());
            }
            if (!response.getAliases().isEmpty()) {
                Optional<String> aliasAncestor = response.getAliases().entrySet().stream()
                        .filter(entry -> entry.getValue().stream().anyMatch(alias -> alias.alias().equals(indexName)))
                        .map(Map.Entry::getKey)
                        .findFirst();

                if(aliasAncestor.isPresent()) {
                    MappingMetadata mapping = response.getMappings().get(aliasAncestor.get());
                    Settings settings = response.getSettings().get(aliasAncestor.get());
                    return FlintMetadata.apply(mapping.source().string(), settings.toString());
                }
            }
            throw new IllegalStateException("Failed to get Flint index metadata for " + osIndexName);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to get Flint index metadata for " + osIndexName, e);
        }
    }

    @Override
    public void deleteIndex(String indexName) {
        String osIndexName = toLowercase(indexName);
        try (RestHighLevelClient client = createClient()) {
            DeleteIndexRequest request = new DeleteIndexRequest(osIndexName);
            AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
            if (!response.isAcknowledged()) {
                throw new IllegalStateException(String.format("Failed to acknowledge delete index %s", indexName));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to delete Flint index " + osIndexName, e);
        }
    }

    /**
     * Create {@link FlintReader}.
     *
     * @param indexName index name.
     * @param query     DSL query. DSL query is null means match_all.
     * @return {@link FlintReader}.
     */
    @Override
    public FlintReader createReader(String indexName, String query) {
        try {
            QueryBuilder queryBuilder = new MatchAllQueryBuilder();
            if (!Strings.isNullOrEmpty(query)) {
                XContentParser
                        parser =
                        XContentType.JSON.xContent().createParser(xContentRegistry, IGNORE_DEPRECATIONS, query);
                queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
            }
            return new OpenSearchScrollReader(createClient(),
                    toLowercase(indexName),
                    new SearchSourceBuilder().query(queryBuilder),
                    options);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public FlintWriter createWriter(String indexName) {
        return new OpenSearchWriter(createClient(), toLowercase(indexName), options.getRefreshPolicy());
    }

    @Override
    public RestHighLevelClient createClient() {
        RestClientBuilder
                restClientBuilder =
                RestClient.builder(new HttpHost(options.getHost(), options.getPort(), options.getScheme()));

        // SigV4 support
        if (options.getAuth().equals(FlintOptions.SIGV4_AUTH)) {
            AWS4Signer signer = new AWS4Signer();
            signer.setServiceName("es");
            signer.setRegionName(options.getRegion());

            // Use DefaultAWSCredentialsProviderChain by default.
            final AtomicReference<AWSCredentialsProvider> awsCredentialsProvider =
                    new AtomicReference<>(new DefaultAWSCredentialsProviderChain());
            String providerClass = options.getCustomAwsCredentialsProvider();
            if (!Strings.isNullOrEmpty(providerClass)) {
                try {
                    Class<?> awsCredentialsProviderClass = Class.forName(providerClass);
                    Constructor<?> ctor = awsCredentialsProviderClass.getDeclaredConstructor();
                    ctor.setAccessible(true);
                    awsCredentialsProvider.set((AWSCredentialsProvider) ctor.newInstance());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            restClientBuilder.setHttpClientConfigCallback(cb ->
                    cb.addInterceptorLast(new AWSRequestSigningApacheInterceptor(signer.getServiceName(),
                            signer, awsCredentialsProvider.get())));
        } else if (options.getAuth().equals(FlintOptions.BASIC_AUTH)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(options.getUsername(), options.getPassword()));
            restClientBuilder.setHttpClientConfigCallback(
                    httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        return new RestHighLevelClient(restClientBuilder);
    }

    /*
     * Because OpenSearch requires all lowercase letters in index name, we have to
     * lowercase all letters in the given Flint index name.
     */
    private String toLowercase(String indexName) {
        Objects.requireNonNull(indexName);

        return indexName.toLowerCase(Locale.ROOT);
    }
}
