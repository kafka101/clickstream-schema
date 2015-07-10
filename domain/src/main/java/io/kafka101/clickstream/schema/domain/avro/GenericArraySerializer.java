package io.kafka101.clickstream.schema.domain.avro;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.lang.reflect.Type;

public class GenericArraySerializer extends StdSerializer<GenericArray> {
    public final static GenericArraySerializer instance = new GenericArraySerializer();

    public GenericArraySerializer() {
        super(GenericArray.class);
    }

    @Override
    public void serialize(GenericArray value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeStartArray();
        serializeContents(value, jgen, provider);
        jgen.writeEndArray();
    }

    @Override
    public void serializeWithType(GenericArray value, JsonGenerator jgen, SerializerProvider provider,
            TypeSerializer typeSer) throws IOException {
        typeSer.writeTypePrefixForArray(value, jgen);
        serializeContents(value, jgen, provider);
        typeSer.writeTypeSuffixForArray(value, jgen);
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint) throws JsonMappingException {
        return createSchemaNode("array", true);
    }

    protected void serializeContents(GenericArray value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException {
        for (Object object : value) {
            if (object == null) {
                jgen.writeNull();
                continue;
            }
            Class<?> cls = object.getClass();
            if (cls == GenericRecord.class) {
                GenericRecordSerializer.instance.serialize((GenericRecord) object, jgen, provider);
            } else if (cls == GenericArray.class) {
                serialize((GenericArray) object, jgen, provider);
            } else if (cls == String.class) {
                jgen.writeString((String) object);
            } else if (cls == Integer.class) {
                jgen.writeNumber(((Integer) object).intValue());
            } else if (cls == Long.class) {
                jgen.writeNumber(((Long) object).longValue());
            } else if (cls == Boolean.class) {
                jgen.writeBoolean(((Boolean) object).booleanValue());
            } else if (cls == Double.class) {
                jgen.writeNumber(((Double) object).doubleValue());
            } else if (GenericRecord.class.isAssignableFrom(cls)) { // sub-class
                GenericRecordSerializer.instance.serialize((GenericRecord) object, jgen, provider);
            } else if (GenericArray.class.isAssignableFrom(cls)) { // sub-class
                serialize((GenericArray) object, jgen, provider);
            } else {
                provider.defaultSerializeValue(object, jgen);
            }
        }
    }
}