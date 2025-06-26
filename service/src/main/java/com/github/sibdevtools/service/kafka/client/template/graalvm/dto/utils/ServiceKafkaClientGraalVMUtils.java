package com.github.sibdevtools.service.kafka.client.template.graalvm.dto.utils;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.HostAccess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author sibmaks
 * @since 0.0.24
 */
@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ServiceKafkaClientGraalVMUtils {
    private final ServiceKafkaClientGraalVMUtilsBase64 base64;
    private final ServiceKafkaClientGraalVMUtilsBinary binary;
    private final ServiceKafkaClientGraalVMUtilsJson json;

    @HostAccess.Export
    public ServiceKafkaClientGraalVMUtilsBase64 base64() {
        return base64;
    }

    @HostAccess.Export
    public ServiceKafkaClientGraalVMUtilsBinary binary() {
        return binary;
    }

    @HostAccess.Export
    public ServiceKafkaClientGraalVMUtilsJson json() {
        return json;
    }


}
