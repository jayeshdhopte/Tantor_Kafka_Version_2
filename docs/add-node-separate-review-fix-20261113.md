# Broker + Controller review blocker - 13 November 2026

The role-selection screen required broker readiness for both Broker and Broker + Controller, but rendered broker check failures/remedies only for Broker. A failed candidate rack check could leave Review selected layout disabled with no visible explanation for the separate-process selection. Controller and separate-process cards and Add Node tooltips also retained obsolete execution-unavailable text.

Live evidence on 194: current preview 30709dfb-842d-417e-8a9f-d0735dc16a3e completed its agent checks, but backend B_RACK failed because the candidate rack was blank while the existing cluster uses rack1. No quorum-expansion validation task or operation had been submitted. Both agent versions were .7 and the backend expansion execution gate was already enabled.

The UI now displays broker checks/remedies for both layouts containing a broker, provides Edit host or broker rack, preserves the selected layout while correcting the rack, and removes the obsolete unavailable labels. Controller-only review still does not require broker-specific checks. No backend gate was bypassed; a new readiness preview is required after changing the rack.

Validation: browser fixture exercises the real four-step wizard, missing-rack rejection and visible remedy, rack correction/fresh checks, separate review entry, controller-only independence, and final validation/deploy gates. It asserts no automatic inspections or mutation requests. Changed-page ESLint and production UI build passed. Run the fixture against Vite on 127.0.0.1:5193 with Playwright available: node scripts/test-add-node-separate-browser.cjs. PLAYWRIGHT_MODULE and CHROME_EXECUTABLE optionally select installed tooling.

Separate-process resource admission is intentionally unchanged. With minDiskGb=20, two processes sharing /opt require 40 GiB available there. The live 191 snapshot had about 35 GiB available, which is sufficient for broker-only readiness but insufficient for the two-process final validation. Increase capacity or select eligible storage/another host before attempting execution; do not silently lower production admission limits. The lab's actual rack must also be supplied when adding a broker.

This correction does not add a broker/controller or authorize static maintenance downtime. Existing Kafka processes and agent binaries/configuration are preserved.
