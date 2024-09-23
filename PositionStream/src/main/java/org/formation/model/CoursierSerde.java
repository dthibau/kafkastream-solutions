package org.formation.model;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class CoursierSerde implements Serde<Coursier> {

    private final CoursierSerializer serializer;
    private final CoursierDeserializer deserializer;

    public CoursierSerde() {
        this.serializer = new CoursierSerializer();
        this.deserializer = new CoursierDeserializer();
    }

    @Override
    public Serializer<Coursier> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Coursier> deserializer() {
        return deserializer;
    }

    // Méthode utilitaire pour créer un Serde de type Coursier
    public static Serde<Coursier> coursierSerde() {
        return Serdes.serdeFrom(new CoursierSerializer(), new CoursierDeserializer());
    }
}
