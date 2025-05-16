package com.github.sibdevtools.service.kafka.client.exception;

import com.github.sibdevtools.error.exception.ServiceException;
import com.github.sibdevtools.service.kafka.client.constant.Constant;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class MessageTemplateNotFoundException extends ServiceException {
    public MessageTemplateNotFoundException(long groupId) {
        super(Constant.ERROR_SOURCE, "MESSAGE_TEMPLATE", "Message template '%d' not found".formatted(groupId));
    }

    public MessageTemplateNotFoundException(String groupCode) {
        super(Constant.ERROR_SOURCE, "MESSAGE_TEMPLATE", "Message template with code '%s' not found".formatted(groupCode));
    }
}
