package com.github.sibdevtools.service.kafka.client.api.dto;

import lombok.*;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class TopicPartitionDto implements Serializable {
    private int partition;
    private String topic;

    public TopicPartitionDto(TopicPartition topicPartition) {
        this.partition = topicPartition.partition();
        this.topic = topicPartition.topic();
    }
}
