package io.kafka101.clickstream.schema.domain.avro;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

public class SerializationModule extends SimpleModule {
    public SerializationModule() {
        addSerializer(GenericArray.class, new GenericArraySerializer());
        addSerializer(GenericRecord.class, new GenericRecordSerializer());
    }
}
