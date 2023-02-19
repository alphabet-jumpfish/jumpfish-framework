package io.github.alphabet.message.listener.core.anotations;


import org.springframework.stereotype.Indexed;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Indexed
public @interface ListenerProcessorStrategy {

    DynamicListenerType strategy();
}
