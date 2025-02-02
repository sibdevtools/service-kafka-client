package com.github.sibdevtools.service.kafka.client.api.rq;

import lombok.*;

import java.io.Serializable;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */

@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class SendTemplateMessageRq implements Serializable {
    private long bootstrapGroupId;
    private String topic;
    private Integer partition;
    private Long timestamp;
    private byte[] key;
    private Map<String, Serializable> input;
    private Map<String, byte[]> headers;
    private Long maxTimeout;
}
