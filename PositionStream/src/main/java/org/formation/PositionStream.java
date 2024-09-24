package org.formation;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.formation.model.CoursierPosition;
import org.formation.model.CoursierStatut;
import org.formation.model.JsonSerde;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class PositionStream {

    public static String APPLICATION_ID = "position-avro";
    public static String BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094";
    public static String REGISTRY_URL = "http://localhost:8081";

    public static String POSITION_TOPIC = "position";
    public static String STATUT_TOPIC = "statutCoursier";
    public static String OUTPUT_TOPIC = "position-avro";

    public static void main(String[] args) {


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        props.put("consumer." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");
        props.put("consumer."+ ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);




        Serde<CoursierPosition> coursierPositionSerde = new JsonSerde<>(CoursierPosition.class);
        Serde<CoursierStatut> coursierStatutSerde = new JsonSerde<>(CoursierStatut.class);


        // Création d’une topolgie de processeurs
        final StreamsBuilder builder = new StreamsBuilder();
        var positionStream = builder.<String, CoursierPosition>stream(POSITION_TOPIC, Consumed.with(Serdes.String(), coursierPositionSerde));
        var statutStream = builder.<String, CoursierStatut>stream(STATUT_TOPIC, Consumed.with(Serdes.String(), coursierStatutSerde));

        KStream<String, String> coursiersPositions = positionStream.join(
                statutStream,
                (coursierPosition, coursierStatut) -> coursierPosition.toString() + coursierStatut.toString(), // Valeur jointe
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)), // Fenêtre de 5 minutes sans période de grâce
                StreamJoined.with(Serdes.String(),
                        Serdes.serdeFrom(coursierPositionSerde.serializer(), coursierPositionSerde.deserializer()),
                        Serdes.serdeFrom(coursierStatutSerde.serializer(), coursierStatutSerde.deserializer()))
        );

        coursiersPositions.to("coursiers-statut-position", Produced.with(Serdes.String(), Serdes.String()));



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


}
