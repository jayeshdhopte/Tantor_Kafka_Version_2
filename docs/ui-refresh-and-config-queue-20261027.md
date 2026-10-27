# UI refresh and configuration queue improvements

- Kafka Connect, Schema Registry and Partitions have refresh-rate controls: Off (default), 5, 10, 30 and 60 seconds. Service pages refresh the selected saved connection using read-only summary requests; automatic discovery/connection writes are not run by the timer. First-time services without a saved endpoint still use their existing Fetch action.
- Polling pauses while requests or edit/confirmation dialogs are active, and while the browser document is hidden. Requests are scheduled after completion; unmount, interval changes and context changes cancel pending timers.
- Configuration change previews retain their existing design with more padding, row spacing and separation between property sections, health status and the action button.
- Consumers no longer renders the Other/unclassified panel. Schema Registry and Kafka Connect retain their service coordination panels.
- External bootstrap entries have separate IP/Hostname and Port controls. Multiple entries and existing host:port API/storage format are preserved, including bracketed IPv6 serialization. Empty/invalid hostnames and ports outside 1-65535 are rejected.
- Batch static configuration preview/apply dispatches full-file reads to all selected nodes before waiting, and reuses each node snapshot across requested properties. Apply still obtains fresh configuration and health data; agents still verify expected values before writing. The UI describes this as Rechecking nodes and queueing. No agent changes.

Checks: 125 UI tests passed; 381 backend tests, 0 failures/errors, 3 optional skips. Production UI build checked before release. No production configuration mutation was used for validation.

Acceptance:
1. Open each of the three tabs, fetch its data if needed, choose 5s and verify new read requests; choose Off to stop. Open an edit dialog and verify polling pauses. Slow endpoints must not accumulate overlapping reads.
2. Change selected service instance, then verify subsequent reads use that instance. Change partition search/page and verify refresh retains those filters.
3. Consumers should contain only the consumer list/empty state; service coordination remains under Schema/Connect.
4. Enter two bootstrap endpoints with separate host and port values. Test the connection. Reject blank ports, 0, 65536, malformed IPv4 and malformed hostnames. Existing external connection settings must split saved endpoints correctly.
5. On a healthy test cluster, review multiple static properties and apply. Verify all selected-node read tasks are queued before the first wait, one file read per target for the batch, and the resulting job still has one grouped change/restart step per node. Timing remains dependent on agent polling, node availability and Kafka health.
