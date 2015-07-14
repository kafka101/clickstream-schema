package io.kafka101.clickstream.schema.consumer;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static kafka.consumer.Consumer.createJavaConsumerConnector;

public class Consumer<T> {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final ConsumerConnector consumerConnector;
    private final Properties configuration;
    private final String topic;
    private final MessageConsumer<T> consumer;
    private ExecutorService pool;

    public Consumer(String zookeeper, String schemaUrl, String groupId, MessageConsumer<T> consumer) {
        this.configuration = getConfiguration(zookeeper, schemaUrl, groupId);
        this.consumerConnector = createJavaConsumerConnector(new ConsumerConfig(configuration));
        this.consumer = consumer;
        this.topic = consumer.getTopic();
    }

    private Properties getConfiguration(String zookeeper, String schemaUrl, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("offsets.storage", "kafka");
        props.put("dual.commit.enabled", "false");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("schema.registry.url", schemaUrl);
        return props;
    }

    public void shutdown() {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
        try {
            shutdownExecutor();
        } catch (InterruptedException e) {
            logger.error("Interrupted during shutdown, exiting uncleanly {}", e);
        }
    }

    private void shutdownExecutor() throws InterruptedException {
        if (pool == null) {
            return;
        }
        pool.shutdown();
        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
            List<Runnable> rejected = pool.shutdownNow();
            logger.debug("Rejected tasks: {}", rejected.size());
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                logger.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        }
    }

    public void run(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap();
        topicCountMap.put(topic, new Integer(numThreads));

        KafkaAvroDecoder avroDecoder = new KafkaAvroDecoder(new VerifiableProperties(configuration));

        Map<String, List<KafkaStream<Object, Object>>> consumerMap = consumerConnector.createMessageStreams(
                topicCountMap, avroDecoder, avroDecoder);

        List<KafkaStream<Object, Object>> streams = consumerMap.get(topic);

        // create fixed size thread pool to launch all the threads
        pool = Executors.newFixedThreadPool(numThreads);

        // create consumer threads to handle the messages
        int threadNumber = 0;

        for (final KafkaStream stream : streams) {
            String name = String.format("%s[%s]", consumer.getTopic(), threadNumber++);
            Runnable runnable = new ConsumerThread(stream, name, consumer);
            pool.submit(runnable);
        }
    }
}
