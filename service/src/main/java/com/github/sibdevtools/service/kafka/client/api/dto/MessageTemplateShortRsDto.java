package com.github.sibdevtools.service.kafka.client.api.dto;

import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;
import com.github.sibdevtools.service.kafka.client.entity.MessageTemplateEntity;
import lombok.*;

import java.io.Serializable;
import java.time.ZonedDateTime;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class MessageTemplateShortRsDto implements Serializable {
    private long id;
    private String code;
    private String name;
    private MessageEngine engine;
    private ZonedDateTime createdAt;
    private ZonedDateTime modifiedAt;

    public MessageTemplateShortRsDto(MessageTemplateEntity entity) {
        this.id = entity.getId();
        this.code = entity.getCode();
        this.name = entity.getName();
        this.engine = entity.getEngine();
        this.createdAt = entity.getCreatedAt();
        this.modifiedAt = entity.getModifiedAt();
    }
}
