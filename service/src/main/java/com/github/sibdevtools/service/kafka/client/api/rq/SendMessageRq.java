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
public class SendMessageRq implements Serializable {
    private Integer partition;
    private Long timestamp;
    private byte[] key;
    private byte[] value;
    private Map<String, byte[]> headers;
    private Integer maxTimeout;
}
