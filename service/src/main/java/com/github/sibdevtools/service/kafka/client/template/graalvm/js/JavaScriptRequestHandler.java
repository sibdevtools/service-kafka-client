package com.github.sibdevtools.service.kafka.client.template.graalvm.js;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;
import com.github.sibdevtools.service.kafka.client.template.graalvm.GraalVMRequestHandler;
import com.github.sibdevtools.session.api.service.SessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * @author sibmaks
 * @since 0.0.7
 */
@Slf4j
@Component
public class JavaScriptRequestHandler extends GraalVMRequestHandler {

    @Autowired
    public JavaScriptRequestHandler(SessionService sessionService,
                                    @Qualifier("kafkaClientServiceObjectMapper")
                                    ObjectMapper objectMapper) {
        super("js", sessionService, objectMapper);
    }

    @Override
    public MessageEngine getEngine() {
        return MessageEngine.JAVA_SCRIPT;
    }
}
