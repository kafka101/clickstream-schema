package io.kafka101.clickstream.schema.domain;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.IOException;

public class AvroRecordDeserializer extends StdDeserializer<GenericData.Record> {

    private Schema schema;
    private AvroArrayDeserializer avroArrayDeserializer;

    public AvroRecordDeserializer(Schema schema) {
        super(GenericData.Record.class);
        this.schema = schema;
        this.avroArrayDeserializer = new AvroArrayDeserializer(schema, this);
    }

    @Override
    public GenericData.Record deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        GenericData.Record ob = new GenericData.Record(this.schema);
        JsonToken t = jp.getCurrentToken();
        if (t == JsonToken.START_OBJECT) {
            t = jp.nextToken();
        }
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();
            t = jp.nextToken();
            switch (t) {
            case START_ARRAY:
                ob.put(fieldName, avroArrayDeserializer.deserialize(jp, ctxt));
                continue;
            case START_OBJECT:
                ob.put(fieldName, deserialize(jp, ctxt));
                continue;
            case VALUE_STRING:
                ob.put(fieldName, jp.getText());
                continue;
            case VALUE_NULL:
                // is nop ok here?
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
}
