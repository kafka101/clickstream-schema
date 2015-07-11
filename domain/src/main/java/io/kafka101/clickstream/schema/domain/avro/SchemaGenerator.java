package io.kafka101.clickstream.schema.domain.avro;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;

public class SchemaGenerator {

    private SchemaGenerator() {
    }

    public static AvroSchema generateAvroSchema(Class clazz) {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator generator = new AvroSchemaGenerator();
        try {
            mapper.acceptJsonFormatVisitor(clazz, generator);
            return generator.getGeneratedSchema();
        } catch (JsonMappingException ex) {
            throw new RuntimeException("Could not generate schema for class: " + clazz, ex);
        }
    }
}
