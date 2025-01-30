package com.github.sibdevtools.service.kafka.client.api.dto;

import lombok.*;
import org.apache.kafka.clients.producer.RecordMetadata;

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
public class RecordMetadataDto implements Serializable {
    private long offset;
    private long timestamp;
    private int serializedKeySize;
    private int serializedValueSize;
    private int partition;

    public RecordMetadataDto(RecordMetadata metadata) {
        this.offset = metadata.offset();
        this.timestamp = metadata.timestamp();
        this.serializedKeySize = metadata.serializedKeySize();
        this.serializedValueSize = metadata.serializedValueSize();
        this.partition = metadata.partition();
    }
}
