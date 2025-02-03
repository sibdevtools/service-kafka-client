package com.github.sibdevtools.service.kafka.client.template;

import com.github.sibdevtools.error.exception.ServiceException;
import com.github.sibdevtools.service.kafka.client.constant.Constant;
import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;
import org.apache.avro.SchemaParser;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Component
public class AvroTemplateMessageEngine implements TemplateMessageEngine {
    @Override
    public RenderedMessage render(
            Integer partition,
            Long timestamp,
            byte[] key,
            byte[] template,
            Map<String, Serializable> input,
            Map<String, byte[]> headers
    ) {
        var schemaParser = new SchemaParser();
        var schema = schemaParser.parse(new String(template, StandardCharsets.UTF_8));
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

        try {
            dataFileWriter.create(mainSchema, outputStream);
            dataFileWriter.append(message);
            dataFileWriter.flush();
            dataFileWriter.close();
            var value = outputStream.toByteArray();
            return SimpleRenderedMessage.builder()
                    .partition(partition)
                    .timestamp(timestamp)
                    .key(key)
                    .value(value)
                    .headers(headers)
                    .build();
        } catch (IOException e) {
            throw new ServiceException(Constant.ERROR_SOURCE, "TEMPLATE_ERROR", "Can't render template", e);
        }
    }

    @Override
    public MessageEngine getEngine() {
        return MessageEngine.AVRO;
    }
}
