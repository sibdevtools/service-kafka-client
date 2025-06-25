package com.github.sibdevtools.service.kafka.client.template.graalvm.python;

import com.github.sibdevtools.service.kafka.client.entity.MessageEngine;
import com.github.sibdevtools.service.kafka.client.template.RenderedMessage;
import com.github.sibdevtools.service.kafka.client.template.TemplateMessageEngine;
import com.github.sibdevtools.service.kafka.client.template.graalvm.GraalVMTemplateMessageEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.7
 */
@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class PythonTemplateMessageEngine implements TemplateMessageEngine {
    private final GraalVMTemplateMessageEngine graalVMTemplateMessageEngine;

    @Override
    public RenderedMessage render(
            Integer partition,
            Long timestamp,
            byte[] key,
            byte[] template,
            Map<String, Serializable> input,
            Map<String, byte[]> headers
    ) {
        return graalVMTemplateMessageEngine.render("python", partition, timestamp, key, template, input, headers);
    }

    @Override
    public MessageEngine getEngine() {
        return MessageEngine.PYTHON;
    }
}
