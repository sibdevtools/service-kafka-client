package com.github.sibdevtools.service.kafka.client.template;

import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.7
 */
public interface RenderedMessage {
    Integer getPartition();

    Long getTimestamp();

    byte[] getKey();

    byte[] getValue();

    Map<String, byte[]> getHeaders();
}
