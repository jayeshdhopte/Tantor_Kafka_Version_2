# Artifact download evidence

`kf_artifact.action = DOWNLOAD_STARTED` records the start of an HTTP transfer.
Its nullable `verified_checksum` is unknown: a successful response alone cannot
prove that a receiving agent verified the bytes. Existing uncorrelated HTTP-start
rows are retained under this event type by V96.

`DOWNLOAD` is a task-correlated receipt. `task_id` uniquely identifies the receipt
and `root_artifact_id` identifies the uploaded binary. The server writes it after
accepting a terminal `DISTRIBUTE_PARCEL` result using the existing claim-token
validation. Verification is true only with the agent's verification log and a
valid expected SHA-256 matching the uploaded artifact. Explicit checksum failure
produces false; missing evidence produces no confirmed receipt.

Request-time task parameters retain `initiated_by`, `target_hostname` and
`target_host_ip`. Receipt `created_by`, `updated_by`, `downloaded_by` and
`user_name` identify that initiator. `executor` separately identifies the agent.
Async parcel completion events preserve the initiating actor and include both
identities in audit details.

V96 reconstructs historical receipts only from retained task evidence and request
audits. It never guesses a host for an uncorrelated HTTP-start row. Original audit
rows remain immutable; `PARCEL_ATTRIBUTION_CORRECTED` events link to the original
completion IDs and preserve their original actor and timestamp in the details.

Validation includes Java regression tests and a PostgreSQL rehearsal covering
successful verification, failure in either logs or the error field, absent
evidence, mismatched hashes, unrelated or unfinished tasks, idempotent replay,
and preservation of old audit entries and artifact binary metadata.
