package com.github.sibdevtools.service.kafka.client.api.dto;

import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */

@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class MessageDto implements Serializable {
    private String topic;
    private int partition;
    private long offset;
    private long timestamp;
    private TimestampType timestampType;
    private Map<String, byte[]> headers;
    private byte[] key;
    private byte[] value;

    public MessageDto(ConsumerRecord<byte[], byte[]> consumerRecord) {
        this.topic = consumerRecord.topic();
        this.partition = consumerRecord.partition();
        this.offset = consumerRecord.offset();
        this.timestamp = consumerRecord.timestamp();
        this.timestampType = consumerRecord.timestampType();
        this.headers = buildHeaders(consumerRecord.headers());
        this.key = consumerRecord.key();
        this.value = consumerRecord.value();
    }

    private Map<String, byte[]> buildHeaders(Headers headers) {
        var headersMap = new HashMap<String, byte[]>();
        for (var header : headers) {
            headersMap.put(header.key(), header.value());
        }
        return headersMap;
    }
}
