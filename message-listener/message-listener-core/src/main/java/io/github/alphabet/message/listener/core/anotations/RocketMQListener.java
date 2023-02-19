package io.github.alphabet.message.listener.core.anotations;

import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Indexed;

import java.lang.annotation.*;

@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
@Indexed
public @interface RocketMQListener {

    String[] topics() default {};

    String concurrency() default "1";

    String subscriptionName() default "default";

    String consumer() default "default";

    MessageModel messageModel() default MessageModel.CLUSTERING;

    int consumeMessageBatchMaxSize() default 1;

    boolean autoCommitACK() default false;

}
