package com.github.sibdevtools.service.kafka.client.template.graalvm.dto;

import com.github.sibdevtools.service.kafka.client.template.graalvm.GraalVMConverter;
import com.github.sibdevtools.session.api.dto.SessionId;
import com.github.sibdevtools.session.api.dto.SessionOwnerType;
import com.github.sibdevtools.session.api.rq.CreateSessionRq;
import com.github.sibdevtools.session.api.service.SessionService;
import jakarta.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.graalvm.polyglot.HostAccess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.7
 */
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ServiceKafkaClientGraalVMSessions {
    private final SessionService sessionService;

    @HostAccess.Export
    public GraalVMSession get(@Nonnull String sessionId) {
        var getSessionRs = sessionService.get(sessionId);
        var session = getSessionRs.getBody();
        return new GraalVMSession(sessionService, session.getId());
    }

    @HostAccess.Export
    public GraalVMSession get(@Nonnull String sessionId, long version) {
        var getSessionRs = sessionService.get(SessionId.of(sessionId, version));
        var session = getSessionRs.getBody();
        return new GraalVMSession(sessionService, session.getId());
    }

    @HostAccess.Export
    public GraalVMSession get(@Nonnull String sessionId, String versionCode) {
        var version = Long.parseLong(versionCode);
        var getSessionRs = sessionService.get(SessionId.of(sessionId, version));
        var session = getSessionRs.getBody();
        return new GraalVMSession(sessionService, session.getId());
    }

    @HostAccess.Export
    public GraalVMSession create(@Nonnull Map<String, Map<String, ?>> sections,
                                 @Nonnull String ownerTypeCode,
                                 @Nonnull String ownerId,
                                 @Nonnull List<String> permissions) {
        var ownerType = SessionOwnerType.valueOf(ownerTypeCode);
        var rq = CreateSessionRq.builder()
                .sections(GraalVMConverter.convertSections(sections))
                .ownerType(ownerType)
                .ownerId(ownerId)
                .permissions(permissions)
                .build();
        var createSessionRs = sessionService.create(rq);
        var sessionId = createSessionRs.getBody();
        var getSessionRs = sessionService.get(sessionId);
        var session = getSessionRs.getBody();
        return new GraalVMSession(sessionService, session.getId());
    }

}
