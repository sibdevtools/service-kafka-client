package com.github.sibdevtools.service.kafka.client.template.graalvm.dto.kvs;

import org.graalvm.polyglot.HostAccess;

/**
 * @author sibmaks
 * @since 0.0.19
 */
public record ValueMetaImpl(
        @HostAccess.Export long createdAt,
        @HostAccess.Export long modifiedAt,
        @HostAccess.Export Long expiredAt,
        @HostAccess.Export long version
) {
}
