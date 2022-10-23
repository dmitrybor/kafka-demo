package com.pydog.demos.kafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    private final static Logger LOGGER = LoggerFactory.getLogger(OpenSearchConsumer.class);

    public static void main(String[] args) throws IOException {
        final String openSearchUrl = "http://localhost:9200";
        final String openSearchIndexName = "wikimedia";
        final String kafkaBootstrapServer = "localhost:9092";
        final String kafkaConsumerGroupId = "consumer-opensearch-demo";
        final String kafkaTopic = "wikimedia.recentchanges";
        final Properties kafkaConsumerProperties = createKafkaConsumerProperties(kafkaBootstrapServer, kafkaConsumerGroupId);

        RestHighLevelClient openSearchClient = createOpenSearchClient(openSearchUrl);
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer(kafkaConsumerProperties);
        try (openSearchClient; kafkaConsumer) {
            createOpenSearchIndex(openSearchClient, openSearchIndexName);
            kafkaConsumer.subscribe(Collections.singleton(kafkaTopic));
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                LOGGER.info("Received {} records", records.count());
                indexIntoOpenSearch(records, openSearchIndexName, openSearchClient);
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

    private static Properties createKafkaConsumerProperties(final String bootstrapServer, final String groupId) {
        LOGGER.info("Creating consumer config...");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return properties;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(Properties properties) {
        LOGGER.info("Creating consumer...");
        return new KafkaConsumer<>(properties);
    }

    private static void createOpenSearchIndex(final RestHighLevelClient client, final String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        boolean isIndexPresent = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (!isIndexPresent) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            LOGGER.info("OpenSearch index '{}' has been created.", indexName);
        } else {
            LOGGER.info("OpenSearch index '{}' already exists.", indexName);
        }
    }

    private static void indexIntoOpenSearch(final ConsumerRecords<String, String> kafkaRecords,
                                            final String openSearchIndexName,
                                            final RestHighLevelClient openSearchClient) {
        kafkaRecords.forEach(record -> {
            IndexRequest indexRequest = new IndexRequest(openSearchIndexName)
                    .source(record.value(), XContentType.JSON);
            try {
                IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                LOGGER.info("Record indexed. Document id: {}", indexResponse.getId());
            } catch (IOException e) {
                LOGGER.error("Error while indexing a document (partition: {}, offset: {}) in OpenSearch",
                        record.partition(), record.offset(), e);
            } catch (Exception e) {
            }
        });
    }
}
