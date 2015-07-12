package io.kafka101.clickstream.schema.producer;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.kafka101.clickstream.schema.domain.Click;
import io.kafka101.clickstream.schema.domain.avro.AvroTranslator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ClickProducer {

    private final String topic;
    private final Schema schema;
    private final KafkaProducer<String, GenericRecord> producer;

    public ClickProducer(String topic, String broker, String schemaRegistry) throws JsonMappingException {
        this.topic = topic;
        this.producer = createProducer(broker, schemaRegistry);
        this.schema = AvroTranslator.schemaFor(Click.class);
    }

    private KafkaProducer<String, GenericRecord> createProducer(String broker, String schemaRegistry) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
        return new KafkaProducer<>(config);
    }

    public RecordMetadata send(Click click) throws IOException, ExecutionException, InterruptedException {
        GenericRecord genericRecord = AvroTranslator.toRecord(click, schema);
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, click.ip, genericRecord);
        return this.producer.send(record).get();
    }

    public void close() {
        producer.close();
    }
}
