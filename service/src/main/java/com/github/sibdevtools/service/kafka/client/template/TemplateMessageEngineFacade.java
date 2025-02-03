package com.github.sibdevtools.service.kafka.client.template;

import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Component
public class TemplateMessageEngineFacade {

    private final EnumMap<MessageEngine, TemplateMessageEngine> messageEngines;

    public TemplateMessageEngineFacade(
            List<TemplateMessageEngine> engines
    ) {
        this.messageEngines = new EnumMap<>(MessageEngine.class);
        engines.forEach(engine -> messageEngines.put(engine.getEngine(), engine));
    }

    public RenderedMessage render(
            MessageEngine engine,
            Integer partition,
            Long timestamp,
            byte[] key,
            byte[] template,
            Map<String, Serializable> input,
            Map<String, byte[]> headers
    ) {
        var templateMessageEngine = messageEngines.get(engine);
        return templateMessageEngine.render(
                partition,
                timestamp,
                key,
                template,
                input,
                headers
        );
    }
}
