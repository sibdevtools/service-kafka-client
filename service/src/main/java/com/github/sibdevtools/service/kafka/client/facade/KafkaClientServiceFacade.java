package com.github.sibdevtools.service.kafka.client.facade;

import com.github.sibdevtools.service.kafka.client.api.dto.*;
import com.github.sibdevtools.service.kafka.client.api.rq.SendMessageRq;
import com.github.sibdevtools.service.kafka.client.service.BootstrapGroupService;
import com.github.sibdevtools.service.kafka.client.service.MessageConsumerService;
import com.github.sibdevtools.service.kafka.client.service.MessagePublisherService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Service
public class KafkaClientServiceFacade {
    private final BootstrapGroupService bootstrapGroupService;
    private final MessageConsumerService messageConsumerService;
    private final MessagePublisherService messagePublisherService;

    public KafkaClientServiceFacade(
            BootstrapGroupService bootstrapGroupService,
            MessageConsumerService messageConsumerService,
            MessagePublisherService messagePublisherService
    ) {
        this.bootstrapGroupService = bootstrapGroupService;
        this.messageConsumerService = messageConsumerService;
        this.messagePublisherService = messagePublisherService;
    }

    public void createBootstrapGroup(
            BootstrapGroupDto rq
    ) {
        bootstrapGroupService.create(rq);
    }

    public void updateBootstrapGroup(
            long id,
            BootstrapGroupDto rq
    ) {
        bootstrapGroupService.update(id, rq);
    }

    public BootstrapGroupRsDto getBootstrapGroup(
            long id
    ) {
        return bootstrapGroupService.get(id);
    }

    public BootstrapGroupRsDto getBootstrapGroupByCode(
            String code
    ) {
        return bootstrapGroupService.getByCode(code);
    }

    public boolean ping(
            long id
    ) {
        return bootstrapGroupService.ping(id);
    }

    public SortedSet<String> getTopicNames(
            long id
    ) {
        return bootstrapGroupService.getTopicNames(id)
                .orElseThrow(() -> new RuntimeException("Can't get topic"));
    }

    public TopicDescriptionDto getTopicDescription(
            long id,
            String topic
    ) {
        return bootstrapGroupService.getTopicDescription(id, topic)
                .map(TopicDescriptionDto::new)
                .orElseThrow(() -> new RuntimeException("Can't get partitions"));
    }

    public List<MessageDto> getMessages(
            long id,
            String topic,
            int maxMessages,
            Long maxTimeout
    ) {
        return messageConsumerService.getMessages(id, topic, maxMessages, maxTimeout)
                .orElseThrow(() -> new RuntimeException("Can't get messages"))
                .stream()
                .map(MessageDto::new)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    public List<MessageDto> getLastNMessages(
            long id,
            String topic,
            int maxMessages,
            Long maxTimeout
    ) {
        return messageConsumerService.getLastNMessages(id, topic, maxMessages, maxTimeout)
                .orElseThrow(() -> new RuntimeException("Can't get last messages"))
                .stream()
                .map(MessageDto::new)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    public RecordMetadataDto sendMessage(
            long id,
            String topic,
            SendMessageRq rq
    ) {
        return messagePublisherService.sendMessage(
                        id,
                        topic,
                        rq.getPartition(),
                        rq.getTimestamp(),
                        rq.getKey(),
                        rq.getValue(),
                        rq.getHeaders(),
                        rq.getMaxTimeout()

                )
                .map(RecordMetadataDto::new)
                .orElseThrow(() -> new RuntimeException("Can't get send message"));
    }
}
