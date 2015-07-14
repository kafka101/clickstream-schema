package io.kafka101.clickstream.schema.consumer;

import io.kafka101.clickstream.schema.domain.Click;
import io.kafka101.clickstream.schema.domain.avro.AvroTranslator;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

    private final KafkaStream messageStream;
    private final MessageConsumer consumer;
    private final String name;

    public ConsumerThread(KafkaStream messageStream, String name, MessageConsumer consumer) {
        this.messageStream = messageStream;
        this.name = name;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(name);
        logger.info("Started consumer thread {}", name);
        ConsumerIterator<String, Object> it = messageStream.iterator();
        while (it.hasNext()) {
            relayMessage(it.next());
        }
        logger.info("Shutting down consumer thread {}", name);
    }

    private void relayMessage(MessageAndMetadata<String, Object> kafkaMessage) {
        logger.debug("Received message with key '{}' and offset '{}' on partition '{}' for topic '{}'",
                kafkaMessage.key(), kafkaMessage.offset(), kafkaMessage.partition(), kafkaMessage.topic());
        GenericData.Record record = (GenericData.Record) kafkaMessage.message();
        Click click = AvroTranslator.toObject(record);
        consumer.consume(click);
    }
}
