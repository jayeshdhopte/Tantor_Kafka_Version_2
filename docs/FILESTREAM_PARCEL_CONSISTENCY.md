# FileStream parcel consistency

The unified Kafka 4.3.0 parcel contained Apache `libs/connect-file-4.3.0.jar`
and different Confluent `connect-file-8.1.1-ccs.jar` copies in `share` and
`connect-plugins`. A fresh deployment consequently conflicted with its own
bundled plugin. Checks compare content, not vendor version numbering.

The corrected parcel keeps the runtime JAR and an identical copy under
`connect-plugins/tantor-filestream`. `tools/repair_filestream_parcel.py` creates
a new file, verifies its runtime version, preserves other plugins, rejects
unknown custom FileStream locations, and records input/output SHA-256 values.

Prevention applies to both agents:

- The artifact repository rejects differing bundled FileStream contents during
  upload, before marking a parcel AVAILABLE.
- Kafka installation validates downloaded parcel contents before extraction.
- Shared-runtime Connect prechecks check bundled and existing plugin copies
  without changing files. Fresh isolated deployments validate the archive
  before privileged installation.
- The shared installer checks every bundled tree, retains identical copies,
  and never overwrites a different customer-supplied plugin.
- Connect deployment still requires Source and Sink classes from the real
  `/connector-plugins` endpoint before success is reported.

Invalid/custom conflicts are intentionally rejected with the conflicting paths
and a migration instruction. They must not be silently forced through.

## Verification

1. Select the corrected Kafka 4.3.0 artifact for a new internal deployment.
   Add Connect; precheck, installation, REST verification and connection save
   should complete. Verify both FileStream Source and Sink in Plugins.
2. Repeat Add Service through an external cluster, selecting the corrected
   artifact and a separate approved installation/plugin directory.
3. In a disposable environment, supply a parcel with differing FileStream JARs.
   Upload must be rejected; older pinned bad parcels must fail before extraction.
4. Put a different custom FileStream JAR in a disposable worker plugin path.
   Shared-runtime precheck must report the exact conflict and preserve the file.
5. Repeat the installer/precheck with identical copies. It must succeed without
   adding another copy or changing other custom connector JARs.

Automated coverage: both Go suites, archive consistency fixtures, shared-runtime
conflict/read-only/retry checks, artifact repository tests, and Python repair
tests. The corrected real parcel is also checked with Kafka's offline plugin
scanner, without starting or stopping production workers.
