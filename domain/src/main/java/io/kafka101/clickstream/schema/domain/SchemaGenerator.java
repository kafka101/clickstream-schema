package io.kafka101.clickstream.schema.domain;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;

public class SchemaGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper(new AvroFactory());
    private static final AvroSchemaGenerator GENERATOR = new AvroSchemaGenerator();

    private SchemaGenerator() {
    }

    public static AvroSchema generateAvroSchema(Class clazz) {
        try {
            MAPPER.acceptJsonFormatVisitor(clazz, GENERATOR);
            return GENERATOR.getGeneratedSchema();
        } catch (JsonMappingException ex) {
            throw new RuntimeException("Could not generate schema for class: " + clazz, ex);
        }
    }
}
