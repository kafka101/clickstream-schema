package io.kafka101.clickstream.schema.domain.avro;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

public class GenericRecordDeserializer extends StdDeserializer<GenericRecord> {

    private final Schema schema;
    public static final GenericRecordDeserializer instance = new GenericRecordDeserializer();

    public GenericRecordDeserializer() {
        super(GenericData.Record.class);
        schema = null;
    }

    public GenericRecordDeserializer(final Schema schema) {
        super(GenericData.Record.class);
        this.schema = schema;
    }

    public GenericRecord deserialize(Schema schema, JsonParser jp, DeserializationContext ctxt) throws IOException {
        GenericRecord ob = new GenericData.Record(schema);
        JsonToken t = jp.getCurrentToken();
        if (t == JsonToken.START_OBJECT) {
            t = jp.nextToken();
        }
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();
            t = jp.nextToken();
            switch (t) {
            case START_ARRAY:
                GenericArray data = GenericArrayDeserializer.instance.deserialize(schema.getField(fieldName).schema(),
                        jp, ctxt);
                ob.put(fieldName, data);
                continue;
            case START_OBJECT:
                ob.put(fieldName, deserialize(schema.getField(fieldName).schema(), jp, ctxt));
                continue;
            case VALUE_STRING:
                ob.put(fieldName, jp.getText());
                continue;
            case VALUE_NULL:
                ob.put(fieldName, null);
                continue;
            case VALUE_TRUE:
                ob.put(fieldName, Boolean.TRUE);
                continue;
            case VALUE_FALSE:
                ob.put(fieldName, Boolean.FALSE);
                continue;
            case VALUE_NUMBER_INT:
                ob.put(fieldName, jp.getNumberValue());
                continue;
            case VALUE_NUMBER_FLOAT:
                ob.put(fieldName, jp.getNumberValue());
                continue;
            case VALUE_EMBEDDED_OBJECT:
                ob.put(fieldName, jp.getEmbeddedObject());
                continue;
            default:
            }

        }
        return ob;
    }

    @Override
    public GenericRecord deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        verifySchemaIsPresent();
        return deserialize(this.schema, jp, ctxt);
    }

    private void verifySchemaIsPresent() {
        if (schema == null) {
            throw new AvroSerializationException("Schema not present!");
        }
    }
}
