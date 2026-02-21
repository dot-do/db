import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 101,
  name: 'experimentation',
  up: `
CREATE DATABASE IF NOT EXISTS growthbook
;
CREATE VIEW IF NOT EXISTS growthbook.experiment_assignments AS
SELECT
  coalesce(ev.data.userId, ev.actor) AS user_id,
  toDateTime(ev.ts) AS timestamp,
  ev.data.experimentId AS experiment_id,
  ev.data.variationId AS variation_id,
  ev.data.variationName AS variation_name,
  ev.data.anonymousId AS anonymous_id,
  ev.data.sessionId AS session_id,
  ev.data.featureFlag AS feature_flag,
  ev.data.allocation AS allocation,
  ev.data.attributes AS attributes,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'experiment.assigned'
;
CREATE DATABASE IF NOT EXISTS flipt
;
CREATE VIEW IF NOT EXISTS flipt.flag_evaluations AS
SELECT
  ev.ts AS timestamp,
  ev.data.flagKey AS flag_key,
  ev.data.variant AS variant,
  toString(ev.data.value) AS value,
  ev.data.reason AS reason,
  ev.data.enabled AS enabled,
  ev.data.userId AS user_id,
  ev.data.contextId AS context_id,
  ev.data.provider AS provider,
  ev.data.setId AS set_id,
  ev.data.version AS flag_version,
  ev.data.ruleId AS rule_id,
  ev.data.source AS source,
  coalesce(ev.data.namespace, 'default') AS namespace_key,
  ev.data.environment AS environment,
  ev.data.appName AS app_name,
  ev.ns AS ns,
  ev.actor AS actor
FROM {database}.events AS ev
WHERE ev.type = 'flag.evaluated'
;
CREATE VIEW IF NOT EXISTS flipt.counter_aggregated_analytics AS
SELECT
  toStartOfMinute(ev.ts) AS timestamp,
  'flag_evaluation_count' AS analytic_name,
  coalesce(ev.data.namespace, 'default') AS namespace_key,
  ev.data.flagKey AS flag_key,
  coalesce(ev.data.reason, 'UNKNOWN_EVALUATION_REASON') AS reason,
  coalesce(ev.data.variant, toString(ev.data.value), '') AS evaluation_value,
  toUInt32(count()) AS value,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'flag.evaluated'
GROUP BY timestamp, namespace_key, flag_key, reason, evaluation_value, ns
`,
  down: `
DROP VIEW IF EXISTS flipt.counter_aggregated_analytics;
DROP VIEW IF EXISTS flipt.flag_evaluations;
DROP VIEW IF EXISTS growthbook.experiment_assignments
`.trim(),
}
