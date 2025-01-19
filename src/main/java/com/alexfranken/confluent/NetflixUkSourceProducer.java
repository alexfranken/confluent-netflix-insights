package com.alexfranken.confluent;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class NetflixUkSourceProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetflixUkSourceProducer.class);

    public static final String PROPS_FILE = "application.properties";
    public static final String KEY_DESTINATION_TOPIC = "netflixinsights.source.topic";
    public static final String KEY_DATASET_PATH = "netflixinsights.source.dataset";

    private final Properties properties;

    public NetflixUkSourceProducer() throws IOException {
        try (InputStream input = this.getClass().getClassLoader()
                .getResourceAsStream(PROPS_FILE)) {
            this.properties = new Properties();
            this.properties.load(input);
            this.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            this.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        }
    }

    public void readSourceAndPublish() throws IOException {
        Path projectRoot = Paths.get("").toAbsolutePath();
        Path datasetPath = projectRoot.resolve(this.properties.getProperty(KEY_DATASET_PATH));
        StringToNetflixUkClickEventAdapter adapter = new StringToNetflixUkClickEventAdapter();

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(this.properties)) {
            AtomicInteger runCount = new AtomicInteger(0);
            try (Stream<String> s = Files.lines(datasetPath)) {
                s.forEach(line -> {
                    try {
                        int curCount = runCount.incrementAndGet();
                        if (curCount == 1 || "".equals(line)) return;
                        GenericRecord record = adapter.adapt(line);
                        LOGGER.debug("record: {}", record);//TODO would not be at info level
                        ProducerRecord<String, GenericRecord> pRecord = new ProducerRecord<>(this.properties.getProperty(KEY_DESTINATION_TOPIC), record);
                        Future<RecordMetadata> future = producer.send(pRecord, (metadata, exception) -> {
                            if (exception != null) {
                                // Handle errors
                                LOGGER.error("Error sending message: {}", exception.getMessage(), exception);
                            } else {
                                LOGGER.debug("Message sent successfully: {}", metadata.toString());
                            }
                        });

                        //future.get();
                        if(curCount % 10000 == 0){
                            LOGGER.info("Flushing producer {}", curCount);
                            producer.flush();
                        }
                    }catch(Exception e){
                        /*
                            Notes:
                             * 'release date' contains value "NOT AVAILABLE" - For now, treat as String
                             * 'genre' can also have "NOT AVAILABLE" - leave alone
                         */
                        LOGGER.error("Exception with input {}", line, e);
                    }
                });
            }
        }
    }

    public static void main(String[] args) throws IOException {
        NetflixUkSourceProducer producer = new NetflixUkSourceProducer();
        producer.readSourceAndPublish();
    }

    /*
     * Java 23 seemed to require -Djava.security.manager=allow per https://github.com/microsoft/mssql-jdbc/issues/2524
    public void verifyConnection() {
        LOGGER.debug("Debug enabled...");
        LOGGER.info("Info...");
        try (AdminClient adminClient = AdminClient.create(this.properties)) {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            KafkaFuture<String> clusterIdFuture = clusterResult.clusterId();
            String clusterId = clusterIdFuture.get();
            LOGGER.info("Kafka cluster ID: " + clusterId);
            // Proceed with Kafka initialization logic
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Kafka: {}", e.getMessage(), e);
            // Handle initialization failure
        }
    }*/

}