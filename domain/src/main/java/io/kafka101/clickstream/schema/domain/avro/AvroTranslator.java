package io.kafka101.clickstream.schema.domain.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class AvroTranslator {

    private final Map<Class<?>, Schema> namespaceLessSchemaCache = new ConcurrentHashMap<>();
    private final Map<Class<?>, Schema> namespacedSchemaCache = new ConcurrentHashMap<>();

    public <T, R extends GenericContainer> R toAvro(T object) {
        Schema schema = namespacelessSchemaFor(object.getClass());
        return toAvro(object, schema);
    }

    public <T extends GenericContainer, R> R toObject(T avro, Class<R> type) {
        Schema schema = namespacedSchemaFor(type);
        return toObject(avro, schema);
    }

    public Schema namespacelessSchemaFor(Class<?> type) {
        return namespaceLessSchemaCache.computeIfAbsent(type, c -> {
            Schema schema = ReflectData.get().getSchema(c);
            // kind of a hack to set an empty namespace :)
            return new Schema.Parser().parse(schema.toString().replace(schema.getNamespace(), ""));
        });
    }

    public Schema namespacedSchemaFor(Class<?> type) {
        return namespacedSchemaCache.computeIfAbsent(type, c -> ReflectData.get().getSchema(c));
    }

    private <T, R extends GenericContainer> R toAvro(T object, Schema schema) {
        try {
            DatumWriter<T> writer = new ReflectDatumWriter<>(schema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writer.write(object, EncoderFactory.get().directBinaryEncoder(out, null));
            DatumReader<R> reader = new GenericDatumReader<>(schema);
            return reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
        } catch (IOException ex) {
            throw new AvroTranslationException("Could not translate Object to GenericContainer: " + ex.getMessage(),
                    ex);
        }
    }

    private <T extends GenericContainer, R> R toObject(T avro, Schema schema) {
        try {
            DatumWriter<T> writer = new GenericDatumWriter<>(schema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writer.write(avro, EncoderFactory.get().directBinaryEncoder(out, null));
            DatumReader<R> reader = new ReflectDatumReader<>(schema);
            return reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
        } catch (IOException ex) {
            throw new AvroTranslationException("Could not translate GenericContainer to Object: " + ex.getMessage(),
                    ex);
        }
    }
}
