package io.kafka101.clickstream.schema.domain.avro;

public class AvroTranslationException extends RuntimeException {
    public AvroTranslationException(String message, Throwable cause) {
        super(message, cause);
    }
}
