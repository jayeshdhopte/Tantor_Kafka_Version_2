package io.translab.tantor.server.dto;

import lombok.Data;

import java.util.List;

public class LdapDTOs {

    @Data
    public static class LdapConfigCreateRequest {
        private boolean enabled;
        private String serverUrl;
        private boolean useSsl;
        private boolean tlsValidateCert;
        private String tlsCaCert;
        private String bindDn;
        private String bindPassword;
        private String userSearchBase;
        private String userSearchFilter;
        private String groupSearchBase;
        private String adminGroupDn;
        private String monitorGroupDn;
        private String defaultRole;
        private int connectionTimeout;
    }

    @Data
    public static class LdapConfigResponse {
        private String id;
        private boolean enabled;
        private String serverUrl;
        private boolean useSsl;
        private boolean tlsValidateCert;
        private boolean tlsCaCertPresent;
        private String bindDn;
        private String userSearchBase;
        private String userSearchFilter;
        private String groupSearchBase;
        private String adminGroupDn;
        private String monitorGroupDn;
        private String defaultRole;
        private int connectionTimeout;
    }

    @Data
    public static class LdapTestRequest {
        private String username;
        private String password;
        private String bindPassword;
    }

    @Data
    public static class LdapTestResponse {
        private boolean success;
        private String message;
        private String userDn;
        private List<String> groups;
        
        public LdapTestResponse(boolean success, String message, String userDn, List<String> groups) {
            this.success = success;
            this.message = message;
            this.userDn = userDn;
            this.groups = groups;
        }
    }
}
