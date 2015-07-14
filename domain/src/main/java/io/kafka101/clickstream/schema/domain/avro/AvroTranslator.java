package io.kafka101.clickstream.schema.domain.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public final class AvroTranslator {

    private AvroTranslator() {
    }

    public static Schema schemaFor(Class<?> clazz) {
        return ReflectData.get().getSchema(clazz);
    }

    public static <T> GenericRecord toRecord(T object, Schema schema) {
        try {
            DatumWriter<T> writer = new ReflectDatumWriter<>(schema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writer.write(object, EncoderFactory.get().directBinaryEncoder(out, null));
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            return reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
        } catch (IOException ex) {
            throw new AvroTranslationException("Could not translate Object to Record: " + ex.getMessage(), ex);
        }
    }

    public static <T> T toObject(GenericRecord record) {
        try {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writer.write(record, EncoderFactory.get().directBinaryEncoder(out, null));
            DatumReader<T> reader = new ReflectDatumReader<>(record.getSchema());
            return reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
        } catch (IOException ex) {
            throw new AvroTranslationException("Could not translate Record to Object: " + ex.getMessage(), ex);
        }
    }
}
