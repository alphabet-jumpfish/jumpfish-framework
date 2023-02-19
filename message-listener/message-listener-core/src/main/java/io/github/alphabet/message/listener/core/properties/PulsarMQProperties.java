package io.github.alphabet.message.listener.core.properties;


import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConditionalOnProperty(prefix = PulsarMQProperties.PULSAR_MQ_PREFIX, value = "address")
@ConfigurationProperties(prefix = PulsarMQProperties.PULSAR_MQ_PREFIX)
public class PulsarMQProperties {

    public static final String PULSAR_MQ_PREFIX = "message-listener.pulsar";

    private String address;
}
