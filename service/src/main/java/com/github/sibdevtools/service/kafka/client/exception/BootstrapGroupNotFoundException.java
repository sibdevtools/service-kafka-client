package com.github.sibdevtools.service.kafka.client.exception;

import com.github.sibdevtools.error.exception.ServiceException;
import com.github.sibdevtools.service.kafka.client.constant.Constant;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class BootstrapGroupNotFoundException extends ServiceException {
    public BootstrapGroupNotFoundException(long groupId) {
        super(Constant.ERROR_SOURCE, "GROUP_NOT_FOUND", "Bootstrap group '%d' not found".formatted(groupId));
    }

    public BootstrapGroupNotFoundException(String groupCode) {
        super(Constant.ERROR_SOURCE, "GROUP_NOT_FOUND", "Bootstrap group with code '%s' not found".formatted(groupCode));
    }
}
