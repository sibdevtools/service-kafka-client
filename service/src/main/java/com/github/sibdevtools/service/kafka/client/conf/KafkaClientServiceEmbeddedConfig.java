package com.github.sibdevtools.service.kafka.client.conf;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.github.sibdevtools.error.mutable.api.source.ErrorLocalizationsJsonSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import javax.sql.DataSource;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@ErrorLocalizationsJsonSource(
        systemCode = "KAFKA_CLIENT_SERVICE",
        iso3Code = "eng",
        path = "classpath:/service/kafka/client/errors/eng.json"
)
@ErrorLocalizationsJsonSource(
        systemCode = "KAFKA_CLIENT_SERVICE",
        iso3Code = "rus",
        path = "classpath:/service/kafka/client/errors/rus.json"
)
@Configuration
@PropertySource("classpath:/service/kafka/client/application.properties")
public class KafkaClientServiceEmbeddedConfig {

    @Bean("kafkaClientServiceObjectMapper")
    public ObjectMapper kafkaClientServiceObjectMapper() {
        return JsonMapper.builder()
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .addModule(new ParameterNamesModule())
                .addModule(new Jdk8Module())
                .addModule(new JavaTimeModule())
                .build();
    }

    @Bean
    public Flyway kafkaClientServiceFlyway(
            KafkaClientServiceEmbeddedFlywayProperties configuration,
            DataSource dataSource
    ) {
        return Flyway.configure()
                .dataSource(dataSource)
                .encoding(configuration.getEncoding())
                .locations(configuration.getLocations())
                .defaultSchema(configuration.getSchema())
                .schemas(configuration.getSchema())
                .placeholders(
                        Map.of(
                                "schema", configuration.getSchema()
                        )
                )
                .load();
    }

    @Bean
    public MigrateResult kafkaClientServiceFlywayMigrateResult(
            @Qualifier("kafkaClientServiceFlyway") Flyway flyway
    ) {
        return flyway.migrate();
    }
}
