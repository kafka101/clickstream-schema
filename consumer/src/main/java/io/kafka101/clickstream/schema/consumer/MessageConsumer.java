package io.kafka101.clickstream.schema.consumer;

/**
 * Consumes an individual message.
 *
 * @param <T>
 */
public interface MessageConsumer<T> {
    /**
     * Implementations of this method should process a message of type T and must be thread-safe.
     *
     * @param message
     */
    void consume(T message);

    /**
     * @return topic this consumer would like to subscribe to
     */
    String getTopic();
}
