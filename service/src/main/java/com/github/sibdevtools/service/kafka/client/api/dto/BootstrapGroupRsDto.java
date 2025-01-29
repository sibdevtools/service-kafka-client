package com.github.sibdevtools.service.kafka.client.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.github.sibdevtools.service.kafka.client.entity.BootstrapGroupEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class BootstrapGroupRsDto implements Serializable {
    private String code;
    private String name;
    private Long maxTimeout;
    private List<String> bootstrapServers;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSS")
    private ZonedDateTime createdAt;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSS")
    private ZonedDateTime modifiedAt;

    public BootstrapGroupRsDto(BootstrapGroupEntity entity) {
        this.code = entity.getCode();
        this.name = entity.getName();
        this.maxTimeout = entity.getMaxTimeout();
        this.bootstrapServers = Collections.unmodifiableList(entity.getBootstrapServers());
        this.createdAt = entity.getCreatedAt();
        this.modifiedAt = entity.getModifiedAt();
    }
}
