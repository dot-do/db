import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 104,
  name: 'posthog',
  up: `
CREATE DATABASE IF NOT EXISTS posthog
;
CREATE OR REPLACE VIEW posthog.events AS
SELECT
  toUUID(ev.id) AS uuid,
  if(ev.type IN ('page', 'pageview'), '$pageview', if(ev.type = 'identify', '$identify', if(ev.type = 'screen', '$screen', if(ev.type = 'group', '$groupidentify', ev.event)))) AS event,
  toString(ev.data.properties) AS properties,
  ULIDStringToDateTime(ev.id) AS timestamp,
  ev.ns AS team_id,
  coalesce(nullIf(ev.data.userId.:String, ''), nullIf(ev.data.anonymousId.:String, ''), ev.actor.id.:String) AS distinct_id,
  '' AS elements_chain,
  ULIDStringToDateTime(ev.id) AS created_at,
  coalesce(ev.data.sessionId, '') AS \`$session_id\`,
  coalesce(ev.data.path, ev.url) AS \`$current_url\`,
  ev.data.properties.browser AS \`$browser\`,
  ev.data.properties.os AS \`$os\`,
  ev.data.properties.device AS \`$device_type\`
FROM {database}.events AS ev
WHERE ev.type IN ('page', 'track', 'identify')
`,
  down: `
DROP VIEW IF EXISTS posthog.events
`.trim(),
}
