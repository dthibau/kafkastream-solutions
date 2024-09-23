package org.formation;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;
import org.formation.model.Coursier;
import org.formation.model.CoursierSerde;
import org.formation.model.Position;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PositionStream {

    public static String APPLICATION_ID = "position-avro";
    public static String BOOTSTRAP_SERVERS = "localhost:19092";
    public static String REGISTRY_URL = "http://localhost:8081";

    public static String INPUT_TOPIC = "position";
    public static String OUTPUT_TOPIC = "position-avro";

    public static void main(String[] args) {


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CoursierSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);

        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", REGISTRY_URL); // URL

        SpecificAvroSerde<Coursier> avroSerde = new SpecificAvroSerde<>();
        avroSerde.configure(config, false); // false pour le "isKey"

        // Utilisation du SerDe Avro dans une topologie Kafka Streams
        Serde<Coursier> valueSerde = Serdes.serdeFrom(avroSerde.serializer(), avroSerde.deserializer());

        // Création d’une topolgie de processeurs
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, Coursier>stream(INPUT_TOPIC)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), valueSerde));

        final Topology topology = builder.build();

// Instanciation du Stream à partir d’une topologie et des propriétés
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        // Démarrage du stream
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static void registerSchema() throws IOException, RestClientException {

        // avro schema avsc file path.
        String schemaPath = "/Courier.avsc";
        // subject convention is "<topic-name>-value"
        String subject = OUTPUT_TOPIC + "-value";
        // avsc json string.
//		String schema = null;

        InputStream inputStream = PositionStream.class.getResourceAsStream(schemaPath);

        Schema avroSchema = new Schema.Parser().parse(inputStream);

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(REGISTRY_URL, 20);

        client.register(subject, new AvroSchema(avroSchema));

    }
}
