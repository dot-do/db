import type { Migration } from '../migrate'

/**
 * Migration 19: Add bloom_filter skip index on cf-ray in events table.
 *
 * Request trace lookups (apis.do/request_{cf-ray}) require searching
 * events by the cf-ray header buried in JSON data. Without an index,
 * this requires a full table scan (~37s on 549K rows).
 *
 * A bloom_filter skip index on the JSON subcolumn path lets ClickHouse
 * skip granules that definitely don't contain the target cf-ray,
 * reducing scan to near-instant.
 */
export const migration: Migration = {
  version: 19,
  name: 'cf_ray_index',
  up: `
ALTER TABLE {database}.events
  ADD INDEX IF NOT EXISTS idx_cf_ray
  data.event.request.headers.\`cf-ray\`::String
  TYPE bloom_filter
  GRANULARITY 4;

ALTER TABLE {database}.events MATERIALIZE INDEX idx_cf_ray
`,
  down: `
ALTER TABLE {database}.events DROP INDEX IF EXISTS idx_cf_ray
`,
}
