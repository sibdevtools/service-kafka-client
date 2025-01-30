package com.github.sibdevtools.service.kafka.client.controller;

import com.github.sibdevtools.common.api.rs.StandardBodyRs;
import com.github.sibdevtools.common.api.rs.StandardRs;
import com.github.sibdevtools.error.exception.ServiceException;
import com.github.sibdevtools.service.kafka.client.api.dto.MessageTemplateDto;
import com.github.sibdevtools.service.kafka.client.api.dto.MessageTemplateRsDto;
import com.github.sibdevtools.service.kafka.client.api.dto.RecordMetadataDto;
import com.github.sibdevtools.service.kafka.client.api.rq.SendTemplateMessageRq;
import com.github.sibdevtools.service.kafka.client.constant.Constant;
import com.github.sibdevtools.service.kafka.client.service.TemplateMessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@RestController
@RequestMapping("/kafka-client-service/message-template")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class MessageTemplateController {
    private final TemplateMessageService templateMessageService;

    @GetMapping("/")
    public StandardRs getAll() {
        var body = templateMessageService.getAll();
        return new StandardBodyRs<>(new ArrayList<>(body));
    }

    @PostMapping("/")
    public StandardBodyRs<Long> create(
            @RequestBody MessageTemplateDto rq
    ) {
        var body = templateMessageService.create(rq);
        return new StandardBodyRs<>(body);
    }

    @GetMapping("/{id}")
    public StandardBodyRs<MessageTemplateRsDto> get(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        var body = templateMessageService.get(id);
        return new StandardBodyRs<>(body);
    }

    @PutMapping("/{id}")
    public StandardRs update(
            @RequestBody MessageTemplateDto rq,
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        templateMessageService.update(id, rq);
        return new StandardRs();
    }

    @DeleteMapping("/{id}")
    public StandardRs delete(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        templateMessageService.delete(id);
        return new StandardRs();
    }

    @PostMapping("/{id}/send")
    public StandardBodyRs<RecordMetadataDto> send(
            @PathVariable("id") String rawId,
            @RequestBody SendTemplateMessageRq rq
    ) {
        var id = Long.parseLong(rawId);
        var body = templateMessageService.send(id, rq)
                .orElseThrow(() -> new ServiceException(Constant.ERROR_SOURCE, "SEND_ERROR", "Can't send message"));
        return new StandardBodyRs<>(body);
    }

}
