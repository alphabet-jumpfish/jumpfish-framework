package io.github.alphabet.message.listener.core.message;

public interface ConsumerRecord<Schema, Payload> {

    Schema schema();

    Payload payload();

}