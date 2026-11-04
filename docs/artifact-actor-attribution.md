# Artifact lifecycle actor attribution (point 74)

For a UI-initiated action, actor columns contain the authenticated initiating **username**, not a fixed role, a configured admin name, or the account that happens to execute a callback. The executor is separate. A principal whose real username is `system` is not rewritten. Authorization roles and actor identity are different concepts.

| Operation | Actor source | Executor |
|---|---|---|
| Upload | Current authorized caller | Repository server |
| Distribution requested/completed/failed | Initiator persisted when scheduling the parcel task | Management server / target agent |
| Agent HTTP download start | Persisted initiating user of that exact distribution task | `agent:<host id>` |
| Direct authenticated HTTP download | Authenticated caller supplied through the configured control-plane proxy | Repository client |
| Verify or delete through UI | Current authorized caller for that operation, which may differ from uploader | Artifact repository |
| Download with no task context or authenticated caller | `unknown` (identity not established) | Repository client |

The earlier point 74 fix correctly attributed confirmed DOWNLOAD receipts and distribution completion audit events, but missed HTTP DOWNLOAD_STARTED rows and repository verify/delete metadata. The download controller still used an unconditional `system` placeholder; verify/delete explicitly passed that same string into metadata creation. Those paths now propagate the actual caller instead.

New distribution tasks carry an opaque random download capability in their artifact URL. The repository resolves it against the server-persisted task, artifact ID and initiating user; it never accepts a username or role from a download query string. Capabilities are artifact-bound and expire after 24 hours. An invalid/expired capability is rejected instead of falling back to an invented actor. The control-plane download proxy preserves this context and Authorization while following no redirects. The repository resolves bearer identity and roles through the management server’s `/api/v1/auth/identity`, using its existing signature/expiry/issuer checks; it does not trust a decoded JWT payload as proof of identity. `TANTOR_AUTH_IDENTITY_URL` configures that internal endpoint (default: the colocated backend on localhost:8443). Existing agents consume the task URL without a binary change.

`created_by`, `updated_by`, `user_name`, and (for downloads) `downloaded_by` consistently identify the action initiator. `executor` records the agent/server; `task_id` ties a transport start to its trusted task when available. STARTED means a response began, not receipt or checksum verification. Only the authenticated task completion/checksum evidence establishes a confirmed DOWNLOAD receipt. Transport start auditing uses `PACKAGE_DOWNLOAD_STARTED`; verification has explicit success/failure audit events. Parcel request/completion audit rows now populate both `user_name` and `created_by`.

Historical transport rows without task context cannot reliably identify who initiated them. They are not globally relabelled as an administrator or correlated by timestamp alone. Existing immutable audit events remain intact, along with earlier evidence-backed attribution corrections. Newly generated records use this contract. System/background operations should only use a service identity when it is actually established, rather than using a blanket substitute for a missing user.

```sql
SELECT id, root_artifact_id, action, created_by, updated_by, user_name,
       downloaded_by, executor, task_id, verified_checksum, created_time
FROM kf_artifact
ORDER BY created_time DESC;

SELECT action,user_name,created_by,created_at,
       details::jsonb->>'initiatedBy' AS initiated_by,
       details::jsonb->>'executor' AS executor,
       details::jsonb->>'taskId' AS task_id
FROM kf_artifact_audit_log
ORDER BY created_at DESC;
```
