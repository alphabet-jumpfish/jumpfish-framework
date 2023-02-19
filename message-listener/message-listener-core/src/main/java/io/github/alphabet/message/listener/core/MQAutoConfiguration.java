package io.github.alphabet.message.listener.core;

import io.github.alphabet.message.listener.core.properties.PulsarMQProperties;
import io.github.alphabet.message.listener.core.properties.RocketMQProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(value = {PulsarMQProperties.class, RocketMQProperties.class})
public class MQAutoConfiguration {

}
