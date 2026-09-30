package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.LdapConfig;
import io.translab.tantor.server.dto.LdapDTOs;
import io.translab.tantor.server.security.EncryptionService;
import org.springframework.stereotype.Service;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

@Service
public class LdapService {
    private static final String ENCRYPTION_PREFIX = "enc:v1:";
    private final EncryptionService encryptionService;

    public LdapService(EncryptionService encryptionService) {
        this.encryptionService = encryptionService;
    }

    /**
     * Create JNDI Environment Properties
     */
    private Hashtable<String, String> createEnv(LdapConfig config, String principal, String credentials) {
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, config.getServerUrl());
        
        if (principal != null) {
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, principal);
            if (credentials != null) {
                env.put(Context.SECURITY_CREDENTIALS, credentials);
            }
        } else {
            env.put(Context.SECURITY_AUTHENTICATION, "none");
        }
        
        if (config.isUseSsl()) {
            env.put(Context.SECURITY_PROTOCOL, "ssl");
            if (!config.isTlsValidateCert()) {
                // To support disable cert validation, would need a custom SocketFactory.
                // For simplicity, we assume standard JSSE truststore is configured.
            }
        }
        env.put("com.sun.jndi.ldap.connect.timeout", String.valueOf(config.getConnectionTimeout() * 1000));
        return env;
    }

    /**
     * Test LDAP connection with bind user
     */
    public LdapDTOs.LdapTestResponse testConnection(LdapConfig config, String bindPassword) {
        try {
            Hashtable<String, String> env = createEnv(config, config.getBindDn(), bindPassword);
            DirContext ctx = new InitialDirContext(env);
            ctx.close();
            return new LdapDTOs.LdapTestResponse(true, "Successfully bound to LDAP server", null, new ArrayList<>());
        } catch (Exception e) {
            return new LdapDTOs.LdapTestResponse(false, "Connection failed: " + e.getMessage(), null, new ArrayList<>());
        }
    }

    /**
     * Authenticate user and fetch groups
     */
    public LdapDTOs.LdapTestResponse authenticate(String username, String password, LdapConfig config, String bindPassword) {
        DirContext bindCtx = null;
        try {
            // 1. Bind with Admin / Bind User
            Hashtable<String, String> env = createEnv(config, config.getBindDn(), bindPassword);
            bindCtx = new InitialDirContext(env);

            // 2. Search for User
            String searchFilter = config.getUserSearchFilter().replace("{username}", username);
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            searchControls.setReturningAttributes(new String[]{"dn"});

            NamingEnumeration<SearchResult> results = bindCtx.search(config.getUserSearchBase(), searchFilter, searchControls);
            
            if (!results.hasMore()) {
                return new LdapDTOs.LdapTestResponse(false, "User not found in directory", null, new ArrayList<>());
            }

            SearchResult searchResult = results.next();
            String userDn = searchResult.getNameInNamespace();

            // 3. Authenticate as the User
            Hashtable<String, String> userEnv = createEnv(config, userDn, password);
            DirContext userCtx = new InitialDirContext(userEnv);
            userCtx.close();

            // 4. Optionally search for groups (if configured)
            List<String> groups = new ArrayList<>();
            if (config.getGroupSearchBase() != null && !config.getGroupSearchBase().isEmpty()) {
                String groupFilter = "(member=" + userDn + ")";
                SearchControls groupControls = new SearchControls();
                groupControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
                groupControls.setReturningAttributes(new String[]{"dn", "cn"});

                NamingEnumeration<SearchResult> groupResults = bindCtx.search(config.getGroupSearchBase(), groupFilter, groupControls);
                while (groupResults.hasMore()) {
                    SearchResult groupResult = groupResults.next();
                    groups.add(groupResult.getNameInNamespace());
                }
            }

            return new LdapDTOs.LdapTestResponse(true, "Authentication successful", userDn, groups);

        } catch (Exception e) {
            return new LdapDTOs.LdapTestResponse(false, "Authentication failed: " + e.getMessage(), null, new ArrayList<>());
        } finally {
            if (bindCtx != null) {
                try {
                    bindCtx.close();
                } catch (NamingException ignored) {}
            }
        }
    }

    public String encryptPassword(String plainText) {
        if (plainText == null) return null;
        return ENCRYPTION_PREFIX + encryptionService.encrypt(plainText);
    }

    public String decryptPassword(String encrypted) {
        if (encrypted == null) return null;
        if (encrypted.startsWith(ENCRYPTION_PREFIX)) {
            return encryptionService.decrypt(encrypted.substring(ENCRYPTION_PREFIX.length()));
        }
        // Read legacy Base64 values once so an administrator can re-save the
        // configuration, which migrates them to AES-GCM encryption.
        return new String(java.util.Base64.getDecoder().decode(encrypted), java.nio.charset.StandardCharsets.UTF_8);
    }
}
