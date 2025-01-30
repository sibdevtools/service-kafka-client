package com.github.sibdevtools.service.kafka.client.controller;

import com.github.sibdevtools.common.api.rs.StandardBodyRs;
import com.github.sibdevtools.common.api.rs.StandardRs;
import com.github.sibdevtools.service.kafka.client.api.dto.MessageTemplateDto;
import com.github.sibdevtools.service.kafka.client.api.dto.RecordMetadataDto;
import com.github.sibdevtools.service.kafka.client.api.rq.SendTemplateMessageRq;
import com.github.sibdevtools.service.kafka.client.facade.KafkaClientServiceFacade;
import org.springframework.web.bind.annotation.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@RestController
@RequestMapping("/kafka-client-service/message-template")
public class MessageTemplateController {
    private final KafkaClientServiceFacade kafkaClientServiceFacade;

    public MessageTemplateController(
            KafkaClientServiceFacade kafkaClientServiceFacade) {
        this.kafkaClientServiceFacade = kafkaClientServiceFacade;
    }

    @PostMapping("/")
    public StandardRs create(
            @RequestBody MessageTemplateDto rq
    ) {
        kafkaClientServiceFacade.createMessageTemplate(rq);
        return new StandardRs();
    }

    @PutMapping("/{id}")
    public StandardRs update(
            @RequestBody MessageTemplateDto rq,
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        kafkaClientServiceFacade.updateMessageTemplate(id, rq);
        return new StandardRs();
    }

    @PostMapping("/{id}/send")
    public StandardBodyRs<RecordMetadataDto> send(
            @PathVariable("id") String rawId,
            @RequestBody SendTemplateMessageRq rq
    ) {
        var id = Long.parseLong(rawId);
        var body = kafkaClientServiceFacade.sendMessageTemplate(
                id,
                rq.getBootstrapGroupId(),
                rq.getTopic(),
                rq.getPartition(),
                rq.getTimestamp(),
                rq.getKey(),
                rq.getInput(),
                rq.getHeaders(),
                rq.getMaxTimeout()
        );
        return new StandardBodyRs<>(body);
    }

}
