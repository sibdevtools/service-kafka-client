package com.github.sibdevtools.service.kafka.client.template.graalvm.dto.kvs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sibdevtools.common.api.rs.StandardBodyRs;
import com.github.sibdevtools.session.api.dto.ValueMeta;
import com.github.sibdevtools.session.api.rq.SetValueRq;
import com.github.sibdevtools.session.api.service.KeyValueStorageService;
import jakarta.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.graalvm.polyglot.HostAccess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.Set;

/**
 * @author sibmaks
 * @since 0.0.19
 */
@Slf4j
@Component
@AllArgsConstructor
public class ServiceKafkaClientGraalVMKeyValueStorage {
    private final KeyValueStorageService keyValueStorageService;

    @Autowired
    @Qualifier("kafkaClientServiceObjectMapper")
    private ObjectMapper objectMapper;

    private static ValueMetaImpl buildGraalMeta(ValueMeta meta) {
        return new ValueMetaImpl(
                meta.getCreatedAt()
                        .toInstant()
                        .toEpochMilli(),
                meta.getModifiedAt()
                        .toInstant()
                        .toEpochMilli(),
                Optional.ofNullable(meta.getExpiredAt())
                        .map(ChronoZonedDateTime::toInstant)
                        .map(Instant::toEpochMilli)
                        .orElse(null),
                meta.getVersion()
        );
    }

    private static ZonedDateTime getExpiredAtDate(SetValueGraalRq rq) {
        val expiredAt = rq.expiredAt();
        if (expiredAt == null) {
            return null;
        }
        val expiredAtInstant = Instant.ofEpochMilli(expiredAt);
        return ZonedDateTime.ofInstant(expiredAtInstant, ZoneOffset.UTC);
    }

    @HostAccess.Export
    public Set<String> getSpaces() {
        return keyValueStorageService.getSpaces();
    }

    @HostAccess.Export
    public Set<String> getKeys(@Nonnull String space) {
        return keyValueStorageService.getKeys(space);
    }

    @HostAccess.Export
    public void delete(@Nonnull String space) {
        keyValueStorageService.delete(space);
    }

    @HostAccess.Export
    public ValueHolderImpl get(@Nonnull String space, @Nonnull String key) {
        return keyValueStorageService.get(space, key)
                .map(StandardBodyRs::getBody)
                .map(it -> {
                    val meta = it.getMeta();
                    return new ValueHolderImpl(
                            it.getValue(),
                            buildGraalMeta(meta)
                    );
                })
                .orElse(null);
    }

    @HostAccess.Export
    public ValueMetaImpl set(
            @Nonnull Object arg
    ) {
        val rq = objectMapper.convertValue(arg, SetValueGraalRq.class);
        val expiredAtDate = getExpiredAtDate(rq);
        val kvsRq = SetValueRq.builder()
                .space(rq.space())
                .key(rq.key())
                .value(rq.value())
                .expiredAt(expiredAtDate)
                .build();
        val rs = keyValueStorageService.set(kvsRq);
        return buildGraalMeta(rs.getBody());
    }

    @HostAccess.Export
    public ValueMetaImpl prolongate(
            @Nonnull String space,
            @Nonnull String key,
            @Nonnull String expiredAt
    ) {
        val expiredAtDate = ZonedDateTime.parse(expiredAt, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        val rs = keyValueStorageService.prolongate(space, key, expiredAtDate);
        return buildGraalMeta(rs.getBody());
    }

    @HostAccess.Export
    public void delete(
            @Nonnull String space,
            @Nonnull String key
    ) {
        keyValueStorageService.delete(space, key);
    }
}
