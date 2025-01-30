package com.github.sibdevtools.service.kafka.client.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;
import com.github.sibdevtools.service.kafka.client.entity.MessageTemplateEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.ZonedDateTime;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class MessageTemplateRsDto {
    private long id;
    private String code;
    private String name;
    private MessageEngine engine;
    private Map<String, Object> schema;
    private byte[] template;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSS")
    private ZonedDateTime createdAt;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSS")
    private ZonedDateTime modifiedAt;

    public MessageTemplateRsDto(MessageTemplateEntity entity, Map<String, Object> schema, byte[] template) {
        this.id = entity.getId();
        this.code = entity.getCode();
        this.name = entity.getName();
        this.engine = entity.getEngine();
        this.schema = schema;
        this.template = template;
        this.createdAt = entity.getCreatedAt();
        this.modifiedAt = entity.getModifiedAt();
    }

}
