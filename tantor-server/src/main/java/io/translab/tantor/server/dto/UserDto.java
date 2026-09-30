package io.translab.tantor.server.dto;

import lombok.Data;
import java.time.OffsetDateTime;
import java.util.UUID;

public class UserDto {

    @Data
    public static class UserCreateRequest {
        private String username;
        private String password;
        private String role;
    }

    @Data
    public static class UserUpdateRequest {
        private String role;
        private String password;
        private Boolean is_active;
    }

    @Data
    public static class UserResponse {
        private UUID id;
        private String username;
        private String role;
        private boolean is_active;
        private OffsetDateTime created_at;
        private OffsetDateTime last_login;
        private String auth_source;
        private String ldap_dn;
    }
}
