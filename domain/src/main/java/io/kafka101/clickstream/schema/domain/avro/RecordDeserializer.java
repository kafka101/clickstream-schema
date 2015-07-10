package io.kafka101.clickstream.schema.domain.avro;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

public class RecordDeserializer extends StdDeserializer<GenericRecord> {

    private Schema schema;

    public RecordDeserializer(Schema schema) {
        super(GenericData.Record.class);
        this.schema = schema;
    }

    @Override
    public GenericRecord deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        GenericRecord ob = new GenericData.Record(this.schema);
        JsonToken t = jp.getCurrentToken();
        if (t == JsonToken.START_OBJECT) {
            t = jp.nextToken();
        }
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();
            t = jp.nextToken();
            switch (t) {
            case START_ARRAY:
                ArrayDeserializer arrayDeserializer = new ArrayDeserializer(schema.getField(fieldName).schema(), this);
                ob.put(fieldName, arrayDeserializer.deserialize(jp, ctxt));
                continue;
            case START_OBJECT:
                GenericRecord inner = new GenericData.Record(schema.getField(fieldName).schema());
                ob.put(fieldName, inner);
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
