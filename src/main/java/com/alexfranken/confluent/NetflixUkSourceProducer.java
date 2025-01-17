package com.alexfranken.confluent;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class NetflixUkSourceProducer {

    public static final String DATASET_PATH = "datasets/netflix-uk/vodclickstream_uk_movies_03.csv";

    public static final String PROPS_FILE = "kafka.properties";
    public static final String DESTINATION_TOPIC = "topic";

    private final ZoneId ukZoneId = ZoneId.of("Europe/London");

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
        Path datasetPath = projectRoot.resolve(DATASET_PATH);
        StringToNetflixUkClickEventAdapter adapter = new StringToNetflixUkClickEventAdapter();

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(this.properties)) {
            AtomicInteger runCount = new AtomicInteger(0);
            try (Stream<String> s = Files.lines(datasetPath)) {
                s.forEach(line -> {
                    try {
                        int curCount = runCount.incrementAndGet();
                        if (curCount == 1 || "".equals(line)) return;
                        GenericRecord record = adapter.adapt(line);
                        System.out.println("record: " + record);
                        //                    producer.send(new ProducerRecord<>(DESTINATION_TOPIC, record));
                    }catch(Exception e){
                        /*
                            Notes:
                             * release date contains value "NOT AVAILABLE" - For now, treat as String
                             * genre can also have "NOT AVAILABLE" - leave alone
                         */
                        System.out.println(line);
                    }
                });
            }
        }
    }

    public static void main(String[] args) throws IOException {
        NetflixUkSourceProducer producer = new NetflixUkSourceProducer();
        producer.readSourceAndPublish();
    }
}