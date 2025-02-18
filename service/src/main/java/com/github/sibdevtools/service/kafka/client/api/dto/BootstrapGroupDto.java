package com.github.sibdevtools.service.kafka.client.api.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class BootstrapGroupDto {
    private String code;
    private String name;
    private Integer maxTimeout;
    private List<String> bootstrapServers;
}
