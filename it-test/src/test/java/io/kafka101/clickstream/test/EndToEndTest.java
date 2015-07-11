package io.kafka101.clickstream.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kafka101.clickstream.schema.domain.Click;
import io.kafka101.clickstream.schema.producer.ClickProducer;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

public class EndToEndTest extends EmbeddedKafkaTest {

    private static final String TOPIC = "clicks";

    @Test
    public void sendAndReceiveTest() throws InterruptedException, ExecutionException, JsonProcessingException {
        ClickProducer producer = new ClickProducer(TOPIC, kafkaConnect, restConnect);
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);
        String nowAsISO = df.format(new Date());
        producer.send(new Click(nowAsISO, "192.168.0.1", "index.html"));
    }

}
