package com.github.sibdevtools.service.kafka.client.template.graalvm.dto.kvs;

/**
 * @author sibmaks
 * @since 0.0.23
 */
public record SetValueGraalRq(
        String space,
        String key,
        byte[] value,
        Long expiredAt
) {
}
