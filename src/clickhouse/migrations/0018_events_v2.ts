import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 18,
  name: 'events_v2',
  up: `
CREATE TABLE IF NOT EXISTS {database}.events_v2 (
  id          String,
  ray         String DEFAULT '',
  ns          String,
  domain      String DEFAULT '',
  type        LowCardinality(String),
  event       LowCardinality(String),
  url         String,
  source      LowCardinality(String),
  actor       JSON,
  data        JSON,
  meta        JSON,
  file        String DEFAULT '',
  ingested    DateTime64(3) DEFAULT now64(3),

  INDEX idx_ns ns TYPE bloom_filter GRANULARITY 4,
  INDEX idx_ray ray TYPE minmax GRANULARITY 4,
  INDEX idx_actor_id actor.id TYPE bloom_filter GRANULARITY 4,
  INDEX idx_actor_org actor.org TYPE bloom_filter GRANULARITY 4
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ULIDStringToDateTime(id))
ORDER BY id
;
INSERT INTO {database}.events_v2
SELECT
  id, ray, ns, '' AS domain,
  type, event, url, source,
  actor, data, meta, file, ingested
FROM {database}.events
;
RENAME TABLE {database}.events TO {database}.events_legacy, {database}.events_v2 TO {database}.events
`,
  down: `
RENAME TABLE {database}.events TO {database}.events_v2, {database}.events_legacy TO {database}.events
;
DROP TABLE IF EXISTS {database}.events_v2
`.trim(),
}
