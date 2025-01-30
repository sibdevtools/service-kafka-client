package com.github.sibdevtools.service.kafka.client.template;

import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;
import gg.jte.CodeResolver;
import gg.jte.ContentType;
import gg.jte.TemplateEngine;
import gg.jte.output.StringOutput;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Component
public class JavaTemplateEngineTemplateMessageEngine implements TemplateMessageEngine {
    @Override
    public byte[] render(byte[] template, Map<String, Object> input) {
        var codeResolver = new CodeResolver() {
            @Override
            public String resolve(String name) {
                return new String(template, StandardCharsets.UTF_8);
            }

            @Override
            public long getLastModified(String name) {
                return System.currentTimeMillis();
            }
        };
        var templateEngine = TemplateEngine.create(codeResolver, ContentType.Plain);

        var output = new StringOutput();
        templateEngine.render("local", input, output);
        return output.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public MessageEngine getEngine() {
        return MessageEngine.JAVA_TEMPLATE_ENGINE;
    }
}
