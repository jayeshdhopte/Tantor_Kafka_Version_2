package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Entity
@Table(name = "kf_ldap_configs")
@Getter
@Setter
public class LdapConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(length = 36)
    private UUID id;

    @Column(nullable = false)
    private boolean enabled = false;

    @Column(name = "server_url", length = 500)
    private String serverUrl;

    @Column(name = "use_ssl", nullable = false)
    private boolean useSsl = false;

    @Column(name = "tls_validate_cert", nullable = false)
    private boolean tlsValidateCert = true;

    @Column(name = "tls_ca_cert", columnDefinition = "TEXT")
    private String tlsCaCert;

    @Column(name = "bind_dn", length = 500)
    private String bindDn;

    @Column(name = "encrypted_bind_password", length = 1000)
    private String encryptedBindPassword;

    @Column(name = "user_search_base", length = 500)
    private String userSearchBase;

    @Column(name = "user_search_filter", length = 500)
    private String userSearchFilter = "(sAMAccountName={username})";

    @Column(name = "group_search_base", length = 500)
    private String groupSearchBase;

    @Column(name = "admin_group_dn", length = 500)
    private String adminGroupDn;

    @Column(name = "monitor_group_dn", length = 500)
    private String monitorGroupDn;

    @Column(name = "default_role", length = 20)
    private String defaultRole = "monitor";

    @Column(name = "connection_timeout")
    private int connectionTimeout = 10;
}
