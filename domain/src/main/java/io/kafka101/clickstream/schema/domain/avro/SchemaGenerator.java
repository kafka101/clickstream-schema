package io.kafka101.clickstream.schema.domain.avro;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;

public class SchemaGenerator {

    private static final AvroMapper MAPPER = new AvroMapper();

    private SchemaGenerator() {
    }

    public static AvroSchema schemaFor(Class<?> clazz) {
        try {
            return MAPPER.schemaFor(clazz);
        } catch (JsonMappingException ex) {
            throw new RuntimeException("Could not generate schema for class: " + clazz, ex);
        }
    }
}
