package io.kafka101.clickstream.schema.domain.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RecordDeserializerTest {

    private static final Logger logger = LoggerFactory.getLogger(RecordDeserializerTest.class);
    private TestPojo pojo;
    private Schema schema;
    private ObjectMapper mapper;

    public RecordDeserializerTest() {
        pojo = new TestPojo(1, 1L, 1.0f, 1.0, "Hello World", new String[]{"Hello World"}, new InnerTestPojo("Hey there"));
        schema = SchemaGenerator.generateAvroSchema(TestPojo.class).getAvroSchema();

        logger.info("Schema: {}", schema.toString(true));

        mapper = new ObjectMapper(new AvroFactory());
        mapper.registerModule(new SimpleModule().addDeserializer(GenericRecord.class, new RecordDeserializer(schema)));
    }

    @Test
    public void testDeserialize() throws Exception {
        GenericRecord genericRecord = mapper.convertValue(pojo, GenericRecord.class);
        assertThat(genericRecord.get("intField"), is(equalTo(pojo.intField)));
        assertThat(genericRecord.get("longField"), is(equalTo(pojo.longField)));
        assertThat(genericRecord.get("floatField"), is(equalTo(pojo.floatField)));
        assertThat(genericRecord.get("doubleField"), is(equalTo(pojo.doubleField)));
        assertThat(genericRecord.get("stringField"), is(equalTo(pojo.stringField)));
        assertThat(((GenericArray) genericRecord.get("stringArray")).get(0), is(equalTo(pojo.stringArray[0])));
        GenericRecord innerRecord = (GenericRecord) genericRecord.get("innerTestPojo");
        assertThat(innerRecord.get("innerStringField"), is(equalTo(pojo.innerTestPojo.innerStringField)));
    }

    public class TestPojo {
        @JsonProperty(required = true)
        public final int intField;

        @JsonProperty(required = true)
        public final long longField;

        @JsonProperty(required = true)
        public final float floatField;

        @JsonProperty(required = true)
        public final double doubleField;

        @JsonProperty(required = true)
        public final String stringField;

        @JsonProperty(required = true)
        public final String[] stringArray;

        @JsonProperty(required = true)
        public final InnerTestPojo innerTestPojo;

        @JsonCreator
        public TestPojo(@JsonProperty("intField") int intField, @JsonProperty("longField") long longField,
                @JsonProperty("floatField") float floatField, @JsonProperty("doubleField") double doubleField,
                @JsonProperty("stringField") String stringField, @JsonProperty("stringArray") String[] stringArray,
                @JsonProperty("innerTestPojo") InnerTestPojo innerTestPojo) {
            this.intField = intField;
            this.longField = longField;
            this.floatField = floatField;
            this.doubleField = doubleField;
            this.stringField = stringField;
            this.stringArray = stringArray;
            this.innerTestPojo = innerTestPojo;
        }
    }

    public class InnerTestPojo {
        @JsonProperty(required = true)
        public final String innerStringField;

        @JsonCreator
        public InnerTestPojo(@JsonProperty("innerStringField") String innerStringField) {
            this.innerStringField = innerStringField;
        }
    }
}