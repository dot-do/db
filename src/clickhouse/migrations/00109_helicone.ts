import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 109,
  name: 'helicone',
  up: `
CREATE DATABASE IF NOT EXISTS helicone
;
CREATE VIEW IF NOT EXISTS helicone.requests AS
SELECT
  o.id AS id,
  o.trace_id AS trace_id,
  o.model AS model,
  o.input AS prompt,
  o.output AS response,
  o.usage['promptTokens'] AS prompt_tokens,
  o.usage['completionTokens'] AS completion_tokens,
  o.usage['totalTokens'] AS total_tokens,
  o.total_cost AS cost,
  if(o.end_time IS NOT NULL, date_diff('millisecond', o.start_time, o.end_time), NULL) AS latency_ms,
  if(o.completion_start_time IS NOT NULL, date_diff('millisecond', o.start_time, o.completion_start_time), NULL) AS time_to_first_token_ms,
  o.status AS status,
  o.level AS level,
  o.metadata['provider'] AS provider,
  o.metadata['cached'] = 'true' AS cached,
  o.ns AS organization_id,
  o.actor AS user_id,
  o.start_time AS created_at
FROM {database}.observations AS o
WHERE o.type = 'generation'
`,
  down: `
DROP VIEW IF EXISTS helicone.requests
`.trim(),
}
