package com.github.sibdevtools.service.kafka.client.template.graalvm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sibdevtools.service.kafka.client.template.RenderedMessage;
import com.github.sibdevtools.service.kafka.client.template.graalvm.dto.GraalVMMessageTemplateContext;
import com.github.sibdevtools.service.kafka.client.template.graalvm.dto.GraalVMRequest;
import com.github.sibdevtools.service.kafka.client.template.graalvm.dto.GraalVMResponse;
import com.github.sibdevtools.service.kafka.client.template.graalvm.dto.ServiceKafkaClientGraalVMSessions;
import com.github.sibdevtools.service.kafka.client.template.graalvm.dto.kvs.ServiceKafkaClientGraalVMKeyValueStorage;
import com.github.sibdevtools.service.kafka.client.template.graalvm.dto.utils.ServiceKafkaClientGraalVMUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.7
 */
@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class GraalVMTemplateMessageEngine {
    private final ServiceKafkaClientGraalVMSessions sessions;
    private final ServiceKafkaClientGraalVMKeyValueStorage keyValueStorage;
    private final ServiceKafkaClientGraalVMUtils graalVMUtils;

    private final Base64.Decoder decoder = Base64.getDecoder();
    private final Base64.Encoder encoder = Base64.getEncoder();

    @Autowired
    @Qualifier("kafkaClientServiceObjectMapper")
    private ObjectMapper objectMapper;

    public RenderedMessage render(
            String language,
            Integer partition,
            Long timestamp,
            byte[] key,
            byte[] template,
            Map<String, Serializable> input,
            Map<String, byte[]> headers
    ) {
        var request = new GraalVMRequest(
                encoder,
                partition,
                timestamp,
                key,
                input,
                headers
        );
        var response = new GraalVMResponse(
                objectMapper,
                decoder,
                partition,
                timestamp,
                key,
                headers
        );
        var context = GraalVMMessageTemplateContext.builder()
                .request(request)
                .response(response)
                .sessions(sessions)
                .keyValueStorage(keyValueStorage)
                .utils(graalVMUtils)
                .build();

        try (var js = Context.newBuilder(language)
                .allowHostAccess(HostAccess.ALL)
                .build()) {
            js.getBindings(language).putMember("wak", context);
            var script = new String(template, StandardCharsets.UTF_8);
            try {
                js.eval(language, script);
            } catch (Exception e) {
                log.error("Template execution exception", e);
            }
        }

        return response.getRenderedMessage();
    }

}
