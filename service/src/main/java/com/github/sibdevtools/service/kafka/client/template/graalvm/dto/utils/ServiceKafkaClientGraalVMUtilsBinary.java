package com.github.sibdevtools.service.kafka.client.template.graalvm.dto.utils;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.HostAccess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author sibmaks
 * @since 0.0.24
 */
@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ServiceKafkaClientGraalVMUtilsBinary {

    @HostAccess.Export
    public byte[] toBytes(Integer value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.allocate(Integer.BYTES)
                .putInt(value)
                .array();
    }

    @HostAccess.Export
    public byte[] toBytes(Long value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.allocate(Long.BYTES)
                .putLong(value)
                .array();
    }

    @HostAccess.Export
    public byte[] toBytes(Float value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.allocate(Float.BYTES)
                .putFloat(value)
                .array();
    }

    @HostAccess.Export
    public byte[] toBytes(Double value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.allocate(Double.BYTES)
                .putDouble(value)
                .array();
    }

    @HostAccess.Export
    public byte[] toBytes(String value) {
        if (value == null) {
            return null;
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @HostAccess.Export
    public Integer toInt(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return ByteBuffer.wrap(bytes).getInt();
    }

    @HostAccess.Export
    public Long toLong(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return ByteBuffer.wrap(bytes).getLong();
    }

    @HostAccess.Export
    public Float toFloat(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return ByteBuffer.wrap(bytes).getFloat();
    }

    @HostAccess.Export
    public Double toDouble(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return ByteBuffer.wrap(bytes).getDouble();
    }

    @HostAccess.Export
    public String toString(byte[] values) {
        if (values == null || values.length == 0) {
            return "";
        }
        return new String(values, StandardCharsets.UTF_8);
    }

}
