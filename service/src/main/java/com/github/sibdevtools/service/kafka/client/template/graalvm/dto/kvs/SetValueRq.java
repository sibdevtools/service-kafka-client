package com.github.sibdevtools.service.kafka.client.template.graalvm.dto.kvs;

import java.io.Serializable;

/**
 * @author sibmaks
 * @since 0.0.19
 */
public record SetValueRq(
        String space,
        String key,
        Serializable value,
        Long expiredAt
) {
}
