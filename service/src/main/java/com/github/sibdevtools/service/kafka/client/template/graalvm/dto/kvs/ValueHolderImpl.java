package com.github.sibdevtools.service.kafka.client.template.graalvm.dto.kvs;

import org.graalvm.polyglot.HostAccess;

/**
 * @author sibmaks
 * @since 0.0.19
 */
public record ValueHolderImpl(
        @HostAccess.Export byte[] value,
        @HostAccess.Export ValueMetaImpl meta
) {
}
