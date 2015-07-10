package io.kafka101.clickstream.schema.domain;

import io.kafka101.clickstream.schema.domain.avro.SchemaGenerator;
import org.apache.avro.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaGeneratorTest {
    private static final Logger logger = LoggerFactory.getLogger(SchemaGeneratorTest.class);

    @Test
    public void testGenerateAvroSchema() throws Exception {
        Schema schema = SchemaGenerator.generateAvroSchema(Click.class).getAvroSchema();
        logger.info(schema.toString());
    }
}