package com.github.sibdevtools.service.kafka.client.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sibdevtools.error.exception.ServiceException;
import com.github.sibdevtools.service.kafka.client.constant.Constant;
import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.10
 */
@Component
public class JsonTemplateMessageEngine implements TemplateMessageEngine {
    private final ObjectMapper objectMapper;

    public JsonTemplateMessageEngine(
            @Qualifier("kafkaClientServiceObjectMapper")
            ObjectMapper objectMapper
    ) {
        this.objectMapper = objectMapper;
    }

    @Override
    public RenderedMessage render(
            Integer partition,
            Long timestamp,
            byte[] key,
            byte[] template,
            Map<String, Serializable> input,
            Map<String, byte[]> headers
    ) {
        try {
            var value = objectMapper.writeValueAsBytes(input);
            return SimpleRenderedMessage.builder()
                    .partition(partition)
                    .timestamp(timestamp)
                    .key(key)
                    .value(value)
                    .headers(headers)
                    .build();
        } catch (IOException e) {
            throw new ServiceException(Constant.ERROR_SOURCE, "TEMPLATE_ERROR", "Can't render template", e);
        }
    }

    @Override
    public MessageEngine getEngine() {
        return MessageEngine.JSON;
    }
}
