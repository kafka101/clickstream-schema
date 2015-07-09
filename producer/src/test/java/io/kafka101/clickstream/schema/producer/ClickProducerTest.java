package io.kafka101.clickstream.schema.producer;

import io.kafka101.clickstream.schema.domain.Click;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ClickProducerTest {

    @Test
    public void testSend() throws Exception {
        ClickProducer producer = new ClickProducer("test.topic2", "127.0.0.1:9092", "http://127.0.0.1:8081");
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);
        String nowAsISO = df.format(new Date());
        producer.send(new Click(nowAsISO, "192.168.0.1", "index.html"));
    }
}