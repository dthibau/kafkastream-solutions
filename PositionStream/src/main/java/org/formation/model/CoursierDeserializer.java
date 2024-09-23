package org.formation.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class CoursierDeserializer implements Deserializer<Coursier>{

	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public Coursier deserialize(String topic, byte[] data) {
		if (data == null)
            return null;
        Coursier ret;
        try {
            ret = objectMapper.readValue(data, Coursier.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
        return ret;
	}

}
