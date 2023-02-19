package io.github.alphabet.message.listener.core.anotations;


import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Indexed;

import java.lang.annotation.*;

@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@MessageMapping
@Indexed
public @interface MQListener {

    RocketMQListener rocketMQListener() default @RocketMQListener;

    PulsarMQListener pulsarMQListener() default @PulsarMQListener;

    String[] topics() default {};

    String concurrency() default "1";

    String consumer() default "default";

}
