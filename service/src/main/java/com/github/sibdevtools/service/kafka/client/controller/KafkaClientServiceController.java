package com.github.sibdevtools.service.kafka.client.controller;

import com.github.sibdevtools.common.api.rs.StandardBodyRs;
import com.github.sibdevtools.common.api.rs.StandardRs;
import com.github.sibdevtools.service.kafka.client.api.dto.*;
import com.github.sibdevtools.service.kafka.client.api.rq.SendMessageRq;
import com.github.sibdevtools.service.kafka.client.facade.KafkaClientServiceFacade;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.TreeSet;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@RestController
@RequestMapping("/kafka-client-service/bootstrap-group")
public class KafkaClientServiceController {
    private final KafkaClientServiceFacade kafkaClientServiceFacade;

    public KafkaClientServiceController(
            KafkaClientServiceFacade kafkaClientServiceFacade) {
        this.kafkaClientServiceFacade = kafkaClientServiceFacade;
    }

    @PostMapping("/")
    public StandardRs create(
            @RequestBody BootstrapGroupDto rq
    ) {
        kafkaClientServiceFacade.createBootstrapGroup(rq);
        return new StandardRs();
    }

    @PutMapping("/{id}")
    public StandardRs update(
            @RequestBody BootstrapGroupDto rq,
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        kafkaClientServiceFacade.updateBootstrapGroup(id, rq);
        return new StandardRs();
    }

    @GetMapping("/{id}")
    public StandardBodyRs<BootstrapGroupRsDto> get(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        var body = kafkaClientServiceFacade.getBootstrapGroup(id);
        return new StandardBodyRs<>(body);
    }

    @GetMapping("/byCode/{code}")
    public StandardBodyRs<BootstrapGroupRsDto> getByCode(
            @PathVariable("code") String code
    ) {
        var body = kafkaClientServiceFacade.getBootstrapGroupByCode(code);
        return new StandardBodyRs<>(body);
    }

    @GetMapping("/{id}/ping")
    public StandardBodyRs<Boolean> ping(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        var body = kafkaClientServiceFacade.ping(id);
        return new StandardBodyRs<>(body);
    }

    @GetMapping("/{id}/topics")
    public StandardBodyRs<TreeSet<String>> getTopicNames(
            @PathVariable("id") String rawId
    ) {
        var id = Long.parseLong(rawId);
        var body = kafkaClientServiceFacade.getTopicNames(id);
        return new StandardBodyRs<>(new TreeSet<>(body));
    }

    @GetMapping("/{id}/{topic}/description")
    public StandardBodyRs<TopicDescriptionDto> getTopicDescription(
            @PathVariable("id") String rawId,
            @PathVariable("topic") String topic
    ) {
        var id = Long.parseLong(rawId);
        var body = kafkaClientServiceFacade.getTopicDescription(id, topic);
        return new StandardBodyRs<>(body);
    }

    @GetMapping("/{id}/{topic}/messages")
    public StandardBodyRs<ArrayList<MessageDto>> getMessages(
            @PathVariable("id") String rawId,
            @PathVariable("topic") String topic,
            @RequestParam(value = "maxMessages", required = false, defaultValue = "10") Integer rawMaxMessages,
            @RequestParam(value = "maxTimeout", required = false) Long maxTimeout
    ) {
        var id = Long.parseLong(rawId);
        var maxMessages = rawMaxMessages == null ? 10 : Math.min(rawMaxMessages, 1000);
        var body = kafkaClientServiceFacade.getMessages(id, topic, maxMessages, maxTimeout);
        return new StandardBodyRs<>(new ArrayList<>(body));
    }

    @GetMapping("/{id}/{topic}/lastMessages")
    public StandardBodyRs<ArrayList<MessageDto>> getLastNMessages(
            @PathVariable("id") String rawId,
            @PathVariable("topic") String topic,
            @RequestParam(value = "maxMessages", required = false, defaultValue = "10") Integer rawMaxMessages,
            @RequestParam(value = "maxTimeout", required = false) Long maxTimeout
    ) {
        var id = Long.parseLong(rawId);
        var maxMessages = rawMaxMessages == null ? 10 : Math.min(rawMaxMessages, 1000);
        var body = kafkaClientServiceFacade.getLastNMessages(id, topic, maxMessages, maxTimeout);
        return new StandardBodyRs<>(new ArrayList<>(body));
    }

    @PostMapping("/{id}/{topic}/message")
    public StandardBodyRs<RecordMetadataDto> sendMessage(
            @PathVariable("id") String rawId,
            @PathVariable("topic") String topic,
            @RequestBody SendMessageRq rq
    ) {
        var id = Long.parseLong(rawId);
        var body = kafkaClientServiceFacade.sendMessage(
                id,
                topic,
                rq
        );
        return new StandardBodyRs<>(body);
    }

}
