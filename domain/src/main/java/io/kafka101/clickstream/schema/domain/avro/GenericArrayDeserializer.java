package io.kafka101.clickstream.schema.domain.avro;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;

import java.io.IOException;

public class GenericArrayDeserializer extends StdDeserializer<GenericArray> {

    private final Schema schema;
    public static final GenericArrayDeserializer instance = new GenericArrayDeserializer();

    public GenericArrayDeserializer() {
        super(GenericArray.class);
        this.schema = null;
    }

    public GenericArrayDeserializer(Schema schema) {
        super(GenericArray.class);
        this.schema = schema;
    }

    @Override
    public GenericArray deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        verifySchemaIsPresent();
        return deserialize(this.schema, jp, ctxt);
    }

    public GenericArray deserialize(Schema schema, JsonParser jp, DeserializationContext ctxt) throws IOException {
        // FIXME lpf: initialization with the length of the array
        GenericArray array = new GenericData.Array<>(8, schema);

        JsonToken t;
        while ((t = jp.nextToken()) != JsonToken.END_ARRAY) {
            switch (t) {
            case START_ARRAY:
                array.add(deserialize(schema, jp, ctxt));
                continue;
            case START_OBJECT:
                array.add(GenericRecordDeserializer.instance.deserialize(schema, jp, ctxt));
                continue;
            case VALUE_STRING:
                array.add(jp.getText());
                continue;
            case VALUE_NULL:
                // is nop ok here?
                continue;
            case VALUE_TRUE:
                array.add(Boolean.TRUE);
                continue;
            case VALUE_FALSE:
                array.add(Boolean.FALSE);
                continue;
            case VALUE_NUMBER_INT:
                array.add(jp.getNumberValue());
                continue;
            case VALUE_NUMBER_FLOAT:
                array.add(jp.getNumberValue());
                continue;
            case VALUE_EMBEDDED_OBJECT:
                array.add(jp.getEmbeddedObject());
                continue;
            default:
                throw ctxt.mappingException("Unrecognized or unsupported JsonToken type: " + t);
            }
        }
        return array;
    }

    private void verifySchemaIsPresent() {
        if (schema == null) {
            throw new AvroSerializationException("Schema not present!");
        }
    }
}
