package com.github.sibdevtools.service.kafka.client.template;

import com.github.sibdevtools.error.exception.ServiceException;
import com.github.sibdevtools.service.kafka.client.constant.Constant;
import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;
import freemarker.template.Configuration;
import freemarker.template.Template;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Component
public class FreemarkerTemplateMessageEngine implements TemplateMessageEngine {
    @Override
    public RenderedMessage render(
            Integer partition,
            Long timestamp,
            byte[] key,
            byte[] template,
            Map<String, Serializable> input,
            Map<String, byte[]> headers
    ) {
        var configuration = new Configuration(Configuration.VERSION_2_3_30);
        var stringTemplate = new String(template, StandardCharsets.UTF_8);
        try {
            var runtimeTemplate = new Template("local", stringTemplate, configuration);
            var byteOutputStream = new ByteArrayOutputStream();
            var writer = new OutputStreamWriter(byteOutputStream);
            runtimeTemplate.process(input, writer);
            writer.flush();
            var value = byteOutputStream.toByteArray();
            return SimpleRenderedMessage.builder()
                    .partition(partition)
                    .timestamp(timestamp)
                    .key(key)
                    .value(value)
                    .headers(headers)
                    .build();
        } catch (Exception e) {
            throw new ServiceException(Constant.ERROR_SOURCE, "TEMPLATE_ERROR", "Can't render template", e);
        }
    }

    @Override
    public MessageEngine getEngine() {
        return MessageEngine.FREEMARKER;
    }
}
