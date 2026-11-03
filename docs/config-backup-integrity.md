# Configuration backup integrity

`kf_config_backup` contains both logical property checkpoints and physical backup artifacts. `FILE_CHECKPOINT` and property-level `DYNAMIC_SNAPSHOT` records with no `backup_uri` are logical recovery metadata, not serialized backup files. Their `checksum_sha256` and `verified_at` remain null. Only records referring to an actual physical file receive file checksums.

Updated internal and external agents compare the saved backup bytes with the captured pre-change source bytes using SHA-256 before allowing configuration mutation. A mismatch or unreadable backup fails the operation before writing the new configuration. The dynamic snapshot sidecar is checked against its exact serialized bytes as well. Receipts include the path, lowercase SHA-256, actual UTC verification time, and method `SOURCE_COPY_MATCH`, without exposing file contents.

The backend binds receipts to the same job, target, and reported backup path. It ignores malformed, unbound, future-dated, and invalid-digest receipts. Replayed evidence preserves the existing digest and verification timestamp; conflicting digests cannot overwrite existing evidence. A path in a task log or successful job status alone never implies checksum verification. Older agents remain compatible but do not produce these integrity receipts.

The integrity columns mean:

| Field | Meaning |
| --- | --- |
| checksum_sha256 | SHA-256 of exact physical bytes; not a checksum of a property value or Java/JSON object |
| verified_at | Actual successful file verification time, not job completion or file creation time |
| verification_method | `SOURCE_COPY_MATCH` for creation-time copy verification; `LEGACY_READBACK_STABILITY` for a later readback baseline |
| integrity_status | `VERIFIED` with physical evidence; `MISSING` after a recorded read finds no file; null without such evidence |
| last_checked_at | Actual physical check time, including unsuccessful checks |

Explicit legacy verification reads each regular file twice, compares SHA-256 and file identity/size/timestamps across reads, and records the current verification time. This establishes a baseline from the time of verification. It does not prove that historical bytes were unchanged before that baseline. Missing historical files retain null checksum and verification timestamps; their missing status and check time are recorded. Original files, paths, job outcomes, and configuration values are not rewritten to manufacture evidence.

Agent version `configchange.10-backupintegrity.1` or later is needed for automatic creation-time receipts. Deploy the matching backend and agent packages together. Agent maintenance must preserve service configuration and registration identity, keep stopped agents stopped, and leave Kafka services running.
