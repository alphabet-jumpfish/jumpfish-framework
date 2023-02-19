package io.github.alphabet.message.listener.core.properties;

import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConditionalOnProperty(prefix = RocketMQProperties.ROCKET_MQ_PREFIX, value = "address")
@ConfigurationProperties(prefix = RocketMQProperties.ROCKET_MQ_PREFIX)
public class RocketMQProperties {

    public static final String ROCKET_MQ_PREFIX = "message-listener.rocket";

    private String address;

    private String userName = "username";

    private String password = "password";

}
