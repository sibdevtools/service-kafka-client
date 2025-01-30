package com.github.sibdevtools.service.kafka.client.conf;

import com.github.sibdevtools.error.exception.ServiceException;
import com.github.sibdevtools.storage.api.service.StorageBucketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Slf4j
@Configuration
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class KafkaClientServiceStartUpListener {
    private final StorageBucketService storageBucketService;

    @Value("${kafka.client.service.props.bucket.code}")
    private String bucketCode;

    @EventListener(ContextRefreshedEvent.class)
    public void contextRefreshedEvent() {
        try {
            storageBucketService.create(bucketCode);
        } catch (ServiceException e) {
            log.warn("Bucket creation error: {}", e.getMessage());
        }
    }
}
