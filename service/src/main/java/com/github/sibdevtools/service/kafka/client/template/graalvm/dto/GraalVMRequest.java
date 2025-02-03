package com.github.sibdevtools.service.kafka.client.template.graalvm.dto;

import org.graalvm.polyglot.HostAccess;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.7
 */
public class GraalVMRequest {
    private final Base64.Decoder decoder;
    private final Base64.Encoder encoder;
    @HostAccess.Export
    public final Integer partition;
    @HostAccess.Export
    public final Long timestamp;
    @HostAccess.Export
    public final byte[] key;
    @HostAccess.Export
    public final Map<String, Serializable> input;
    @HostAccess.Export
    public final Map<String, byte[]> headers;

    /**
     * Construct graalvm http request
     *
     * @param partition request partition
     * @param timestamp request timestamp
     * @param key       request key
     * @param input     request input
     * @param headers   request headers
     */
    public GraalVMRequest(
            Base64.Decoder decoder,
            Base64.Encoder encoder,
            Integer partition,
            Long timestamp,
            byte[] key,
            Map<String, Serializable> input,
            Map<String, byte[]> headers
    ) {
        this.decoder = decoder;
        this.encoder = encoder;
        this.partition = partition;
        this.timestamp = timestamp;
        this.key = key;
        this.input = input == null ? Map.of() : Collections.unmodifiableMap(input);
        this.headers = headers == null ? Map.of() : Collections.unmodifiableMap(headers);
    }

    /**
     * Get request input parameter value by name
     *
     * @param name parameter name
     * @return parameter value or null if not found
     */
    @HostAccess.Export
    public Serializable param(String name) {
        return input.get(name);
    }

    /**
     * Get request key as a decoded string
     *
     * @return raw key as a decoded string or null if not found
     */
    @HostAccess.Export
    public Serializable rawKey() {
        return key == null ? null : decoder.decode(key);
    }

    /**
     * Get request header raw value by name
     *
     * @param name header name
     * @return header raw value or null if not found
     */
    @HostAccess.Export
    public String rawHeader(String name) {
        var value = headers.get(name);
        if (value == null) {
            return null;
        }
        return new String(decoder.decode(value), StandardCharsets.UTF_8);
    }

    /**
     * Get request header binary value by name
     *
     * @param name header name
     * @return header binary value or null if not found
     */
    @HostAccess.Export
    public String binaryHeader(String name) {
        var value = headers.get(name);
        if (value == null) {
            return null;
        }
        return encoder.encodeToString(value);
    }

}
