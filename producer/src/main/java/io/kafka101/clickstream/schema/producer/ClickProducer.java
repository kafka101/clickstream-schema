package io.kafka101.clickstream.schema.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.kafka101.clickstream.schema.domain.Click;
import io.kafka101.clickstream.schema.domain.avro.GenericRecordDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ClickProducer {

    private final AvroMapper mapper = new AvroMapper();
    private final String topic;
    private final KafkaProducer<String, GenericRecord> producer;

    public ClickProducer(String topic, String broker, String schemaRegistry) throws JsonMappingException {
        this.topic = topic;
        this.producer = createProducer(broker, schemaRegistry);
        Schema schema = mapper.schemaFor(Click.class).getAvroSchema();
        mapper.registerModule(
                new SimpleModule().addDeserializer(GenericRecord.class, new GenericRecordDeserializer(schema)));
    }

    private KafkaProducer<String, GenericRecord> createProducer(String broker, String schemaRegistry) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
        return new KafkaProducer<>(config);
    }

    public RecordMetadata send(Click click) throws ExecutionException, InterruptedException, JsonProcessingException {
        GenericRecord genericRecord = mapper.convertValue(click, GenericRecord.class);
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, click.ip, genericRecord);
        return this.producer.send(record).get();
    }

    public void close() {
        producer.close();
    }
}
