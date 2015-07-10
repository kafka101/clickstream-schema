package io.kafka101.clickstream.schema.domain.avro;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.lang.reflect.Type;

public class GenericRecordSerializer extends StdSerializer<GenericRecord> {
    public final static GenericRecordSerializer instance = new GenericRecordSerializer();

    public GenericRecordSerializer() {
        super(GenericRecord.class);
    }

    @Override
    public void serialize(GenericRecord value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeStartObject();
        serializeContents(value, jgen, provider);
        jgen.writeEndObject();
    }

    @Override
    public void serializeWithType(GenericRecord value, JsonGenerator jgen, SerializerProvider provider,
            TypeSerializer typeSer) throws IOException, JsonGenerationException {
        typeSer.writeTypePrefixForObject(value, jgen);
        serializeContents(value, jgen, provider);
        typeSer.writeTypeSuffixForObject(value, jgen);
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint) throws JsonMappingException {
        return createSchemaNode("record", true);
    }

    protected void serializeContents(GenericRecord value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException {
        for (Schema.Field field : value.getSchema().getFields()) {
            Object object = value.get(field.name());
            if (object == null) {
                if (provider.isEnabled(SerializationFeature.WRITE_NULL_MAP_VALUES)) {
                    jgen.writeNullField(field.name());
                }
                continue;
            }
            jgen.writeFieldName(field.name());
            Class<?> cls = object.getClass();
            if (cls == GenericRecord.class) {
                serialize((GenericRecord) object, jgen, provider);
            } else if (cls == GenericArray.class) {
                GenericArraySerializer.instance.serialize((GenericArray) object, jgen, provider);
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
                serialize((GenericRecord) object, jgen, provider);
            } else if (GenericArray.class.isAssignableFrom(cls)) { // sub-class
                GenericArraySerializer.instance.serialize((GenericArray) object, jgen, provider);
            } else {
                provider.defaultSerializeValue(object, jgen);
            }
        }
    }
}