CREATE TABLE IF NOT EXISTS ldap_configs (
    id UUID PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    server_url VARCHAR(500),
    use_ssl BOOLEAN NOT NULL DEFAULT FALSE,
    tls_validate_cert BOOLEAN NOT NULL DEFAULT TRUE,
    tls_ca_cert TEXT,
    bind_dn VARCHAR(500),
    encrypted_bind_password VARCHAR(1000),
    user_search_base VARCHAR(500),
    user_search_filter VARCHAR(500) DEFAULT '(sAMAccountName={username})',
    group_search_base VARCHAR(500),
    admin_group_dn VARCHAR(500),
    monitor_group_dn VARCHAR(500),
    default_role VARCHAR(20) DEFAULT 'monitor',
    connection_timeout INT DEFAULT 10
);

ALTER TABLE users ADD COLUMN IF NOT EXISTS auth_source VARCHAR(20) DEFAULT 'local';
ALTER TABLE users ADD COLUMN IF NOT EXISTS ldap_dn VARCHAR(500);
