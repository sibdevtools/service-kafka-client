package com.github.sibdevtools.service.kafka.client.api.dto;

import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class MessageTemplateDto {
    private String code;
    private String name;
    private MessageEngine engine;
    private Map<String, String> headers;
    private Map<String, Object> schema;
    private byte[] template;

}
