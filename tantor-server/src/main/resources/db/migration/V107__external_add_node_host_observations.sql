-- Read-only external host observations. This is not an operation/reservation ledger.
CREATE TABLE kf_external_add_node_observations (
    agent_id varchar(255) PRIMARY KEY REFERENCES kf_discovery_agents(id) ON DELETE CASCADE,
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    report text NOT NULL CHECK (octet_length(report) <= 16384)
);
