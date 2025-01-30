package com.github.sibdevtools.service.kafka.client.template;

import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;

import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface TemplateMessageEngine {

    byte[] render(byte[] template, Map<String, Object> input);

    MessageEngine getEngine();

}
