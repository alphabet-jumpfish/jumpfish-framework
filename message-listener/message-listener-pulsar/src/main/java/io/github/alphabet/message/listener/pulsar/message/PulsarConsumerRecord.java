package io.github.alphabet.message.listener.pulsar.message;


import io.github.alphabet.message.listener.core.message.ConsumerRecord;
import org.apache.pulsar.client.api.Message;


public class PulsarConsumerRecord implements ConsumerRecord<Object, String> {

    private final Object schema;
    private final String payload;
    private final Message message;

    public PulsarConsumerRecord(Object schema, String payload, Message message) {
        this.schema = schema;
        this.payload = payload;
        this.message = message;
    }

    @Override
    public Object schema() {
        return schema;
    }

    @Override
    public String payload() {
        return payload;
    }

    public Message message() {
        return message;
    }
}
