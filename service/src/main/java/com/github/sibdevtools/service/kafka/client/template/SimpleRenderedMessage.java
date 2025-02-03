package com.github.sibdevtools.service.kafka.client.template;

import lombok.*;

import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.7
 */
@Setter
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SimpleRenderedMessage implements RenderedMessage {
    private Integer partition;
    private Long timestamp;
    private byte[] key;
    private byte[] value;
    private Map<String, byte[]> headers;
}
