package com.github.sibdevtools.service.kafka.client.template.graalvm.dto;

import com.github.sibdevtools.service.kafka.client.template.graalvm.dto.kvs.ServiceKafkaClientGraalVMKeyValueStorage;
import com.github.sibdevtools.service.kafka.client.template.graalvm.dto.utils.ServiceKafkaClientGraalVMUtils;
import lombok.Builder;
import org.graalvm.polyglot.HostAccess;

/**
 * @author sibmaks
 * @since 0.0.7
 */
@Builder
public record GraalVMMessageTemplateContext(
        @HostAccess.Export GraalVMRequest request,
        @HostAccess.Export GraalVMResponse response,
        @HostAccess.Export ServiceKafkaClientGraalVMSessions sessions,
        @HostAccess.Export ServiceKafkaClientGraalVMKeyValueStorage keyValueStorage,
        @HostAccess.Export ServiceKafkaClientGraalVMUtils utils
) {
}
