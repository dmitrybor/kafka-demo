package com.pydog.demos.kafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class OpenSearchConsumer {
    private final static Logger LOGGER = LoggerFactory.getLogger(OpenSearchConsumer.class);

    public static void main(String[] args) throws IOException {
        final String openSearchUrl = "http://localhost:9200";
        final String indexName = "wikimedia";


        try (RestHighLevelClient openSearchClient = createOpenSearchClient(openSearchUrl)) {

            GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
            boolean isIndexPresent = openSearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            if (!isIndexPresent) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                LOGGER.info("OpenSearch index '{}' has been created.", indexName);
            } else {
                LOGGER.info("OpenSearch index '{}' already exists.", indexName);
            }
        }
    }

    private static RestHighLevelClient createOpenSearchClient(final String url) {
        final URI uri = URI.create(url);
        final String userInfo = uri.getUserInfo();

        RestHighLevelClient client;
        if (userInfo == null) {
            client = new RestHighLevelClient(RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme())));
        } else {
            String[] auth = userInfo.split(":");
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()));
            restClientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder -> {
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        httpAsyncClientBuilder.setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy());
                        return httpAsyncClientBuilder;
                    }
            );
            client = new RestHighLevelClient(restClientBuilder);
        }
        return client;
    }
}
