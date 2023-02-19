package io.github.alphabet.message.listener.core.anotations;

import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Indexed;

import java.lang.annotation.*;

@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
@Indexed
public @interface PulsarMQListener {

    String[] topics() default {};

    String concurrency() default "1";

    String consumer() default "default";

    String subscriptionName() default "default";

    SubscriptionType subscriptionType() default SubscriptionType.Shared;

    boolean autoCommitACK() default false;

}
