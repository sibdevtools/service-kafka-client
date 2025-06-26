package com.github.sibdevtools.service.kafka.client.template.graalvm.dto.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.HostAccess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author sibmaks
 * @since 0.0.24
 */
@Slf4j
@Component
public class ServiceKafkaClientGraalVMUtilsJson {

    @Autowired
    @Qualifier("kafkaClientServiceObjectMapper")
    private ObjectMapper objectMapper;

    @HostAccess.Export
    public byte[] serialize(Object value) throws JsonProcessingException {
        if (value == null) {
            return null;
        }
        return objectMapper.writeValueAsBytes(value);
    }

    @HostAccess.Export
    public String dump(Object value) throws JsonProcessingException {
        if (value == null) {
            return null;
        }
        return objectMapper.writeValueAsString(value);
    }

    @HostAccess.Export
    public Object parse(String value) throws IOException {
        if (value == null) {
            return null;
        }
        return objectMapper.readValue(value, Object.class);
    }

    @HostAccess.Export
    public Object deserialize(byte[] value) throws IOException {
        if (value == null) {
            return null;
        }
        return objectMapper.readValue(value, Object.class);
    }
}
