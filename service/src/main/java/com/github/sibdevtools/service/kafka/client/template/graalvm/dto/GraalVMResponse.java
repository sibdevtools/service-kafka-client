package com.github.sibdevtools.service.kafka.client.template.graalvm.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sibdevtools.error.exception.ServiceException;
import com.github.sibdevtools.service.kafka.client.constant.Constant;
import com.github.sibdevtools.service.kafka.client.template.RenderedMessage;
import com.github.sibdevtools.service.kafka.client.template.SimpleRenderedMessage;
import org.apache.avro.SchemaParser;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.graalvm.polyglot.HostAccess;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class GraalVMResponse {
    private final ObjectMapper objectMapper;
    private final Base64.Decoder decoder;
    private Integer partition;
    private Long timestamp;
    private byte[] key;
    private byte[] value;
    private Map<String, byte[]> headers;

    public GraalVMResponse(
            ObjectMapper objectMapper,
            Base64.Decoder decoder,
            Integer partition,
            Long timestamp,
            byte[] key,
            Map<String, byte[]> headers
    ) {
        this.objectMapper = objectMapper;
        this.decoder = decoder;
        this.partition = partition;
        this.timestamp = timestamp;
        this.key = key;
        this.headers = headers;
    }

    @HostAccess.Export
    public void partition(Integer partition) {
        this.partition = partition;
    }

    @HostAccess.Export
    public void timestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @HostAccess.Export
    public void header(String key, String value) {
        if (headers == null) {
            headers = new LinkedHashMap<>();
        }
        var rawValue = value.getBytes(StandardCharsets.UTF_8);
        headers.put(key, rawValue);
    }

    @HostAccess.Export
    public void binaryHeader(String key, String value) {
        if (headers == null) {
            headers = new LinkedHashMap<>();
        }
        headers.put(key, decoder.decode(value));
    }

    @HostAccess.Export
    public void plainValue(String body) {
        value = body.getBytes(StandardCharsets.UTF_8);
    }

    @HostAccess.Export
    public void bytesValue(byte[] bytes) {
        value = bytes;
    }

    @HostAccess.Export
    public void jsonValue(Object json) {
        try {
            value = objectMapper.writeValueAsBytes(json);
        } catch (IOException e) {
            throw new ServiceException(Constant.ERROR_SOURCE, "UNEXPECTED_ERROR", "Can't convert json value", e);
        }
    }

    @HostAccess.Export
    public void avroValue(Object rawSchema, Map<String, Object> input) {
        try {
            String schemaJson;
            if (rawSchema instanceof String rawSchemaString) {
                schemaJson = rawSchemaString;
            } else {
                schemaJson = objectMapper.writeValueAsString(rawSchema);
            }
            var schemaParser = new SchemaParser();
            var schema = schemaParser.parse(schemaJson);
            var mainSchema = schema.mainSchema();

            var outputStream = new ByteArrayOutputStream();
            var datumWriter = new GenericDatumWriter<>(mainSchema);
            var dataFileWriter = new DataFileWriter<>(datumWriter);

            var recordBuilder = new GenericRecordBuilder(mainSchema);
            for (var entry : input.entrySet()) {
                var field = mainSchema.getField(entry.getKey());
                if (field != null) {
                    recordBuilder.set(field, entry.getValue());
                }
            }
            var message = recordBuilder.build();

            dataFileWriter.create(mainSchema, outputStream);
            dataFileWriter.append(message);
            dataFileWriter.flush();
            dataFileWriter.close();
            value = outputStream.toByteArray();
        } catch (IOException e) {
            throw new ServiceException(Constant.ERROR_SOURCE, "UNEXPECTED_ERROR", "Can't convert json value", e);
        }
    }

    @HostAccess.Export
    public void plainKey(String body) {
        key = body.getBytes(StandardCharsets.UTF_8);
    }

    @HostAccess.Export
    public void bytesKey(byte[] bytes) {
        key = bytes;
    }

    @HostAccess.Export
    public void jsonKey(Object json) {
        try {
            key = objectMapper.writeValueAsBytes(json);
        } catch (IOException e) {
            throw new ServiceException(Constant.ERROR_SOURCE, "UNEXPECTED_ERROR", "Can't convert json key", e);
        }
    }

    public RenderedMessage getRenderedMessage() {
        return SimpleRenderedMessage.builder()
                .partition(partition)
                .timestamp(timestamp)
                .key(key)
                .value(value)
                .headers(headers)
                .build();
    }
}
