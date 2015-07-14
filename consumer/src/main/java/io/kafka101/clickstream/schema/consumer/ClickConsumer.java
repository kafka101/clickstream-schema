package io.kafka101.clickstream.schema.consumer;

import io.kafka101.clickstream.schema.domain.Click;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickConsumer implements MessageConsumer<Click> {

    private static final Logger logger = LoggerFactory.getLogger(ClickConsumer.class);
    private final String topic;

    public ClickConsumer(String topic) {
        this.topic = topic;
    }

    @Override
    public void consume(Click click) {
        logger.info("Received message {}", click);
    }

    @Override
    public String getTopic() {
        return topic;
    }

}
