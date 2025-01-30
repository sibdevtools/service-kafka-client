package com.github.sibdevtools.service.kafka.client.conf;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Setter
@Getter
@Configuration
@NoArgsConstructor
@AllArgsConstructor
@ConfigurationProperties("spring.flyway.service-kafka-client")
public class KafkaClientServiceFlywayProperties {
    private String encoding;
    private String[] locations;
    private String schema;
}
