package org.formation;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.formation.model.Coursier;
import org.formation.model.CoursierSerde;
import org.formation.model.Position;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Stream1 {

    public static void main(String[] args) {


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CoursierSerde.class);

        // Création d’une topolgie de processeurs
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, Coursier>stream("position")
                .mapValues(c ->{
                    BigDecimal lat = new BigDecimal(c.getCurrentPosition().getLatitude());
                    lat = lat.setScale(1, RoundingMode.HALF_UP);  // Arrondir à 1 décimale
                    BigDecimal lon = new BigDecimal(c.getCurrentPosition().getLongitude());
                    lon = lon.setScale(1, RoundingMode.HALF_UP);  // Arrondir à 1 décimale
                    c.setCurrentPosition(new Position(lat.doubleValue(), lon.doubleValue()));
                    return c;
                })
                .to("round-position");

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
