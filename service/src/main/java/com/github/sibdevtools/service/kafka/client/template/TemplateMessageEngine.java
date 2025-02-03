package com.github.sibdevtools.service.kafka.client.template;

import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;

import java.io.Serializable;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface TemplateMessageEngine {

    RenderedMessage render(
            Integer partition,
            Long timestamp,
            byte[] key,
            byte[] template,
            Map<String, Serializable> input,
            Map<String, byte[]> headers
    );

    MessageEngine getEngine();

}
