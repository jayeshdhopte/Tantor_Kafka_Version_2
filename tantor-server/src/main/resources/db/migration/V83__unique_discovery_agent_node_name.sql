CREATE UNIQUE INDEX ux_kf_discovery_agents_node_name_ci
    ON kf_discovery_agents (lower(btrim(hostname)))
    WHERE hostname IS NOT NULL AND btrim(hostname) <> '';
