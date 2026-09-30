package io.translab.tantor.server.service;

import io.translab.tantor.server.dto.AclDTOs.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class SecurityOperationsService {

    private final KafkaAdminService kafkaAdminService;

    public AclListResponse listAcls(UUID clusterId, String principal, String resourceType, String resourceName) {
        AdminClient adminClient = kafkaAdminService.getAdminClient(clusterId);
        
        ResourcePatternFilter resourceFilter = new ResourcePatternFilter(
                parseResourceType(resourceType, ResourceType.ANY),
                resourceName != null ? resourceName : null,
                PatternType.ANY
        );

        AclBindingFilter filter = new AclBindingFilter(
                resourceFilter,
                new AccessControlEntryFilter(
                        principal != null ? principal : null,
                        null, // host
                        AclOperation.ANY,
                        AclPermissionType.ANY
                )
        );

        try {
            Collection<AclBinding> bindings = adminClient.describeAcls(filter).values().get();
            List<AclEntry> entries = bindings.stream().map(b -> {
                AclEntry entry = new AclEntry();
                entry.setPrincipal(b.entry().principal());
                entry.setHost(b.entry().host());
                entry.setOperation(b.entry().operation().name());
                entry.setPermissionType(b.entry().permissionType().name());
                entry.setResourceType(b.pattern().resourceType().name());
                entry.setResourceName(b.pattern().name());
                entry.setPatternType(b.pattern().patternType().name());
                return entry;
            }).collect(Collectors.toList());
            
            return new AclListResponse(entries);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Error fetching ACLs for cluster {}", clusterId, e);
            throw new RuntimeException("Failed to fetch ACLs: the request was interrupted.", e);
        } catch (ExecutionException e) {
            log.error("Error fetching ACLs for cluster {}", clusterId, e);
            throw kafkaFailure("fetch ACLs", e);
        }
    }

    public AclCreateResponse createAcl(UUID clusterId, AclCreateRequest request) {
        AdminClient adminClient = kafkaAdminService.getAdminClient(clusterId);
        
        ResourcePattern pattern = new ResourcePattern(
                parseResourceType(request.getResource_type(), ResourceType.TOPIC),
                request.getResource_name(),
                parsePatternType(request.getPattern_type(), PatternType.LITERAL)
        );
        
        AccessControlEntry entry = new AccessControlEntry(
                request.getPrincipal(),
                request.getHost() != null && !request.getHost().isEmpty() ? request.getHost() : "*",
                parseOperation(request.getOperation(), AclOperation.ALL),
                parsePermission(request.getPermission_type(), AclPermissionType.ALLOW)
        );
        
        AclBinding binding = new AclBinding(pattern, entry);
        try {
            adminClient.createAcls(List.of(binding)).all().get();
            return new AclCreateResponse(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Error creating ACL for cluster {}", clusterId, e);
            throw new RuntimeException("Failed to create ACL: the request was interrupted.", e);
        } catch (ExecutionException e) {
            log.error("Error creating ACL for cluster {}", clusterId, e);
            throw kafkaFailure("create ACL", e);
        }
    }

    public AclDeleteResponse deleteAcl(UUID clusterId, AclDeleteRequest request) {
        AdminClient adminClient = kafkaAdminService.getAdminClient(clusterId);
        
        ResourcePatternFilter resourceFilter = new ResourcePatternFilter(
                parseResourceType(request.getResource_type(), ResourceType.ANY),
                request.getResource_name() != null ? request.getResource_name() : null,
                parsePatternType(request.getPattern_type(), PatternType.ANY)
        );

        AccessControlEntryFilter entryFilter = new AccessControlEntryFilter(
                request.getPrincipal() != null ? request.getPrincipal() : null,
                request.getHost() != null ? request.getHost() : null,
                parseOperation(request.getOperation(), AclOperation.ANY),
                parsePermission(request.getPermission_type(), AclPermissionType.ANY)
        );
        
        AclBindingFilter filter = new AclBindingFilter(resourceFilter, entryFilter);
        
        try {
            Collection<AclBinding> deletedBindings = adminClient.deleteAcls(List.of(filter)).all().get();
            List<AclEntry> entries = deletedBindings.stream().map(b -> {
                AclEntry entry = new AclEntry();
                entry.setPrincipal(b.entry().principal());
                entry.setHost(b.entry().host());
                entry.setOperation(b.entry().operation().name());
                entry.setPermissionType(b.entry().permissionType().name());
                entry.setResourceType(b.pattern().resourceType().name());
                entry.setResourceName(b.pattern().name());
                entry.setPatternType(b.pattern().patternType().name());
                return entry;
            }).collect(Collectors.toList());
            
            return new AclDeleteResponse(entries);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Error deleting ACL for cluster {}", clusterId, e);
            throw new RuntimeException("Failed to delete ACL: the request was interrupted.", e);
        } catch (ExecutionException e) {
            log.error("Error deleting ACL for cluster {}", clusterId, e);
            throw kafkaFailure("delete ACL", e);
        }
    }

    private RuntimeException kafkaFailure(String operation, ExecutionException exception) {
        Throwable cause = exception.getCause() != null ? exception.getCause() : exception;
        String reason;
        if (cause instanceof SecurityDisabledException) {
            reason = "Kafka ACLs are disabled because no authorizer is configured on the cluster.";
        } else if (cause instanceof AuthorizationException) {
            reason = "the Kafka user used by Tantor is not authorized to manage ACLs.";
        } else if (cause instanceof TimeoutException) {
            reason = "Kafka did not respond before the request timed out. Check broker connectivity.";
        } else if (cause.getMessage() != null && !cause.getMessage().isBlank()) {
            reason = cause.getMessage();
        } else {
            reason = cause.getClass().getSimpleName();
        }
        return new RuntimeException("Failed to " + operation + ": " + reason, cause);
    }

    private ResourceType parseResourceType(String type, ResourceType defaultType) {
        if (type == null) return defaultType;
        try { return ResourceType.valueOf(type.toUpperCase()); } 
        catch (IllegalArgumentException e) { return defaultType; }
    }

    private PatternType parsePatternType(String type, PatternType defaultType) {
        if (type == null) return defaultType;
        try { return PatternType.valueOf(type.toUpperCase()); } 
        catch (IllegalArgumentException e) { return defaultType; }
    }

    private AclOperation parseOperation(String op, AclOperation defaultOp) {
        if (op == null) return defaultOp;
        try { return AclOperation.valueOf(op.toUpperCase()); } 
        catch (IllegalArgumentException e) { return defaultOp; }
    }

    private AclPermissionType parsePermission(String perm, AclPermissionType defaultPerm) {
        if (perm == null) return defaultPerm;
        try { return AclPermissionType.valueOf(perm.toUpperCase()); } 
        catch (IllegalArgumentException e) { return defaultPerm; }
    }
}
