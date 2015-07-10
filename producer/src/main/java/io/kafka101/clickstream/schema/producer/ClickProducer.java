package io.kafka101.clickstream.schema.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.kafka101.clickstream.schema.domain.Click;
import io.kafka101.clickstream.schema.domain.avro.SchemaGenerator;
import io.kafka101.clickstream.schema.domain.avro.GenericRecordDeserializer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ClickProducer {

    private static final Logger logger = LoggerFactory.getLogger(ClickProducer.class);
    private final KafkaProducer<String, GenericRecord> producer;
    private final String topic;
    private final ObjectMapper mapper = new ObjectMapper(new AvroFactory());
    private final AvroSchema schema;

    public ClickProducer(String topic, String broker, String schemaRegistry) {
        this.producer = createProducer(broker, schemaRegistry);
        this.topic = topic;
        this.schema = SchemaGenerator.generateAvroSchema(Click.class);
        mapper.registerModule(new SimpleModule().addDeserializer(GenericRecord.class,
                new GenericRecordDeserializer(schema.getAvroSchema())));
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
        GenericRecord genericRecord = mapper.convertValue(click, GenericData.Record.class);
        logger.info("Schema: '{}'", genericRecord.getSchema().toString(true));
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, click.ip, genericRecord);
        return this.producer.send(record).get();
    }

    public void close() {
        producer.close();
    }
}
