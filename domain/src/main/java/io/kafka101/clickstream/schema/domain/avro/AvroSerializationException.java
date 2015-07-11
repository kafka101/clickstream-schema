package io.kafka101.clickstream.schema.domain.avro;

public class AvroSerializationException extends RuntimeException {

    public AvroSerializationException(String message) {
        super(message);
    }
}
