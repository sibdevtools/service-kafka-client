package com.github.sibdevtools.service.kafka.client.template.graalvm.dto;

import com.github.sibdevtools.service.kafka.client.template.graalvm.GraalVMConverter;
import com.github.sibdevtools.session.api.ModificationQueryBuilder;
import com.github.sibdevtools.session.api.dto.SessionId;
import com.github.sibdevtools.session.api.rq.GetSessionAttributeNamesRq;
import com.github.sibdevtools.session.api.rq.GetSessionAttributeRq;
import com.github.sibdevtools.session.api.rq.UpdateSessionRq;
import com.github.sibdevtools.session.api.service.SessionService;
import lombok.AllArgsConstructor;
import org.graalvm.polyglot.HostAccess;

import java.io.Serializable;
import java.util.Set;

/**
 * @author sibmaks
 * @since 0.0.7
 */
@AllArgsConstructor
public class GraalVMSession {
    private final SessionService sessionService;
    private SessionId sessionId;

    @HostAccess.Export
    public SessionId getId() {
        return sessionId;
    }

    @HostAccess.Export
    public Serializable get(String section, String attribute) {
        var rq = GetSessionAttributeRq.builder()
                .sessionId(sessionId)
                .section(section)
                .attribute(attribute)
                .build();

        return sessionService.getAttribute(rq)
                .getBody();
    }

    @HostAccess.Export
    public Set<String> getAttributeNames(String section) {
        var rq = GetSessionAttributeNamesRq.builder()
                .sessionId(sessionId)
                .section(section)
                .build();

        return sessionService.getAttributeNames(rq)
                .getBody();
    }

    @HostAccess.Export
    public void add(String section, String attribute, Object data) {
        var serialized = GraalVMConverter.toSerializable(data);
        var modificationQuery = ModificationQueryBuilder.builder()
                .create(section, attribute, serialized)
                .build();
        var rq = UpdateSessionRq.builder()
                .sessionId(sessionId)
                .modificationQuery(modificationQuery)
                .build();
        var updateRs = sessionService.update(rq);
        sessionId = updateRs.getBody();
    }

    @HostAccess.Export
    public void set(String section, String attribute, Object data) {
        var serialized = GraalVMConverter.toSerializable(data);
        var modificationQuery = ModificationQueryBuilder.builder()
                .createOrChange(section, attribute, serialized)
                .build();
        var rq = UpdateSessionRq.builder()
                .sessionId(sessionId)
                .modificationQuery(modificationQuery)
                .build();
        var updateRs = sessionService.update(rq);
        sessionId = updateRs.getBody();
    }

    @HostAccess.Export
    public void remove(String section, String attribute) {
        var modificationQuery = ModificationQueryBuilder.builder()
                .remove(section, attribute)
                .build();
        var rq = UpdateSessionRq.builder()
                .sessionId(sessionId)
                .modificationQuery(modificationQuery)
                .build();
        var updateRs = sessionService.update(rq);
        sessionId = updateRs.getBody();
    }
}
