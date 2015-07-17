package io.kafka101.clickstream.test;

import io.kafka101.clickstream.schema.consumer.ClickConsumer;
import io.kafka101.clickstream.schema.consumer.Consumer;
import io.kafka101.clickstream.schema.consumer.MessageConsumer;
import io.kafka101.clickstream.schema.domain.Click;
import io.kafka101.clickstream.schema.producer.Producer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EndToEndTest extends EmbeddedKafkaTest {

    private static final Logger logger = LoggerFactory.getLogger(EndToEndTest.class);
    private static final String TOPIC = "clicks";
    private static final int THREADS = 1;
    private static final int NUMBER_OF_MESSAGES = 50;

    @Test
    public void sendAndReceiveTest() throws InterruptedException, ExecutionException, IOException {

        createTopic(TOPIC);

        final TestClickConsumer clickConsumer = new TestClickConsumer();
        Consumer<Click> consumer = new Consumer(zkConnect, restConnect, "test-group", clickConsumer);
        consumer.run(THREADS);

        Producer producer = new Producer(TOPIC, kafkaConnect, restConnect);
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);

        logger.info("Sending {} messages", NUMBER_OF_MESSAGES);
        long start = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            String nowAsISO = df.format(new Date());
            producer.send(new Click(nowAsISO, "192.168.0.1", "index.html"));
        }
        logger.info("Sent {} messages in {}ms", NUMBER_OF_MESSAGES, System.currentTimeMillis() - start);

        await().atMost(5, TimeUnit.SECONDS).until(() -> clickConsumer.getMessageCount() == NUMBER_OF_MESSAGES);

        assertThat(clickConsumer.getNews().get(0).ip, is(equalTo("192.168.0.1")));

        producer.close();
        consumer.shutdown();
    }

    public class TestClickConsumer extends ClickConsumer implements MessageConsumer<Click> {

        private List<Click> clicks = Collections.synchronizedList(new ArrayList());

        public TestClickConsumer() {
            super(TOPIC);
        }

        @Override
        public void consume(Click click) {
            super.consume(click);
            clicks.add(click);
        }

        public int getMessageCount() {
            return clicks.size();
        }

        public List<Click> getNews() {
            return Collections.unmodifiableList(clicks);
        }
    }
}
