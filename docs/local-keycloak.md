# Local Keycloak for development

The local identity provider runs at `http://localhost:8180` and is bound to
loopback only. It uses a separate `tantor_keycloak` database in the existing
`tantor-postgres` container. The Tantor application database remains `tantor`.

The imported realm has the name `Gatekeeper`, a public `apb-kafka` OIDC client,
PKCE-compatible authorization code flow, localhost UI redirects, and `Admin`
and `Monitor` realm roles. Local users are created separately. Remote users,
secrets, and other private realm settings are not available from the supplied
environment variables; an export from the remote Keycloak is needed to copy
those settings.

Start or stop the identity provider from the repository root:

```powershell
docker compose -f compose.keycloak.local.yml up -d
docker compose -f compose.keycloak.local.yml down
```

The local secrets are in `.env.keycloak.local`, which Git ignores. Do not use
this HTTP development setup outside the local machine. The realm import runs
only when `Gatekeeper` does not already exist in its database; editing the JSON
does not overwrite an existing realm.

After starting Keycloak, the admin console is at
`http://localhost:8180/admin/`. Tantor uses `http://localhost:5173/`.
