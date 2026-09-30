package io.translab.tantor.server.dto;

import java.util.List;

public class AclDTOs {

    public static class AclEntry {
        private String principal;
        private String host;
        private String operation;
        private String permissionType;
        private String resourceType;
        private String resourceName;
        private String patternType;

        public String getPrincipal() { return principal; }
        public void setPrincipal(String principal) { this.principal = principal; }
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }
        public String getPermissionType() { return permissionType; }
        public void setPermissionType(String permissionType) { this.permissionType = permissionType; }
        public String getResourceType() { return resourceType; }
        public void setResourceType(String resourceType) { this.resourceType = resourceType; }
        public String getResourceName() { return resourceName; }
        public void setResourceName(String resourceName) { this.resourceName = resourceName; }
        public String getPatternType() { return patternType; }
        public void setPatternType(String patternType) { this.patternType = patternType; }
    }

    public static class AclCreateRequest {
        private String resource_type;
        private String resource_name;
        private String pattern_type;
        private String principal;
        private String host;
        private String operation;
        private String permission_type;

        public String getResource_type() { return resource_type; }
        public void setResource_type(String resource_type) { this.resource_type = resource_type; }
        public String getResource_name() { return resource_name; }
        public void setResource_name(String resource_name) { this.resource_name = resource_name; }
        public String getPattern_type() { return pattern_type; }
        public void setPattern_type(String pattern_type) { this.pattern_type = pattern_type; }
        public String getPrincipal() { return principal; }
        public void setPrincipal(String principal) { this.principal = principal; }
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }
        public String getPermission_type() { return permission_type; }
        public void setPermission_type(String permission_type) { this.permission_type = permission_type; }
    }

    public static class AclCreateResponse {
        private int acls_added;
        
        public AclCreateResponse(int acls_added) { this.acls_added = acls_added; }
        public int getAcls_added() { return acls_added; }
        public void setAcls_added(int acls_added) { this.acls_added = acls_added; }
    }

    public static class AclDeleteRequest {
        private String resource_type;
        private String resource_name;
        private String pattern_type;
        private String principal;
        private String host;
        private String operation;
        private String permission_type;

        public String getResource_type() { return resource_type; }
        public void setResource_type(String resource_type) { this.resource_type = resource_type; }
        public String getResource_name() { return resource_name; }
        public void setResource_name(String resource_name) { this.resource_name = resource_name; }
        public String getPattern_type() { return pattern_type; }
        public void setPattern_type(String pattern_type) { this.pattern_type = pattern_type; }
        public String getPrincipal() { return principal; }
        public void setPrincipal(String principal) { this.principal = principal; }
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }
        public String getPermission_type() { return permission_type; }
        public void setPermission_type(String permission_type) { this.permission_type = permission_type; }
    }

    public static class AclDeleteResponse {
        private List<AclEntry> deleted_acls;
        
        public AclDeleteResponse(List<AclEntry> deleted_acls) { this.deleted_acls = deleted_acls; }
        public List<AclEntry> getDeleted_acls() { return deleted_acls; }
        public void setDeleted_acls(List<AclEntry> deleted_acls) { this.deleted_acls = deleted_acls; }
    }

    public static class AclListResponse {
        private List<AclEntry> acls;
        
        public AclListResponse(List<AclEntry> acls) { this.acls = acls; }
        public List<AclEntry> getAcls() { return acls; }
        public void setAcls(List<AclEntry> acls) { this.acls = acls; }
    }
}
