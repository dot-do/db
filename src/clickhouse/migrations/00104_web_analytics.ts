import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 104,
  name: 'web_analytics',
  up: `
CREATE DATABASE IF NOT EXISTS plausible
;
CREATE VIEW IF NOT EXISTS plausible.events AS
SELECT
  toDateTime(ev.ts) AS timestamp,
  if(ev.type = 'page', 'pageview', ev.event) AS name,
  ev.ns AS site_id,
  cityHash64(coalesce(ev.data.userId, ev.data.anonymousId, ev.actor)) AS user_id,
  cityHash64(coalesce(ev.data.sessionId, '')) AS session_id,
  extractURLParameter(ev.url, 'host') AS hostname,
  coalesce(ev.data.path, ev.url) AS pathname,
  ev.data.referrer AS referrer,
  ev.data.properties.utm_source AS utm_source,
  ev.data.properties.utm_medium AS utm_medium,
  ev.data.properties.utm_campaign AS utm_campaign,
  ev.data.properties.utm_content AS utm_content,
  ev.data.properties.utm_term AS utm_term,
  ev.data.properties.country AS country_code,
  ev.data.properties.browser AS browser,
  ev.data.properties.os AS operating_system,
  ev.data.properties.device AS screen_size,
  ev.ns AS namespace
FROM {database}.events AS ev
WHERE ev.type IN ('page', 'track')
;
CREATE DATABASE IF NOT EXISTS openpanel
;
CREATE VIEW IF NOT EXISTS openpanel.events AS
SELECT
  ev.id AS id,
  ev.event AS name,
  ev.source AS sdk_name,
  coalesce(ev.data.anonymousId, '') AS device_id,
  coalesce(ev.data.userId, ev.actor) AS profile_id,
  ev.ns AS project_id,
  coalesce(ev.data.sessionId, '') AS session_id,
  coalesce(ev.data.path, ev.url) AS path,
  ev.data.referrer AS referrer,
  CAST(ev.data.properties, 'Map(String, String)') AS properties,
  ev.ts AS created_at,
  ev.data.properties.country AS country,
  ev.data.properties.city AS city,
  ev.data.properties.region AS region,
  ev.data.properties.os AS os,
  ev.data.properties.browser AS browser,
  ev.data.properties.device AS device
FROM {database}.events AS ev
WHERE ev.type IN ('page', 'track')
;
CREATE DATABASE IF NOT EXISTS umami
;
CREATE VIEW IF NOT EXISTS umami.website_events AS
SELECT
  toUUID(ev.id) AS event_id,
  ev.ns AS website_id,
  toUUID(coalesce(ev.data.sessionId, ev.id)) AS session_id,
  toUUID(coalesce(ev.data.sessionId, ev.id)) AS visit_id,
  coalesce(ev.data.path, ev.url) AS url_path,
  '' AS url_query,
  ev.data.referrer AS referrer_domain,
  ev.data.properties.browser AS browser,
  ev.data.properties.os AS os,
  ev.data.properties.device AS device,
  ev.data.properties.language AS language,
  ev.data.properties.country AS country,
  if(ev.type = 'page', 1, 2) AS event_type,
  if(ev.type = 'track', ev.event, '') AS event_name,
  ev.data.properties.title AS page_title,
  coalesce(ev.data.userId, ev.data.anonymousId, ev.actor) AS distinct_id,
  toDateTime(ev.ts, 'UTC') AS created_at
FROM {database}.events AS ev
WHERE ev.type IN ('page', 'track')
;
CREATE DATABASE IF NOT EXISTS posthog
;
CREATE VIEW IF NOT EXISTS posthog.events AS
SELECT
  toUUID(ev.id) AS uuid,
  if(ev.type = 'page', '$pageview', ev.event) AS event,
  toString(ev.data.properties) AS properties,
  ev.ts AS timestamp,
  ev.ns AS team_id,
  coalesce(ev.data.userId, ev.data.anonymousId, ev.actor) AS distinct_id,
  '' AS elements_chain,
  ev.ts AS created_at,
  coalesce(ev.data.sessionId, '') AS \`$session_id\`,
  coalesce(ev.data.path, ev.url) AS \`$current_url\`,
  ev.data.properties.browser AS \`$browser\`,
  ev.data.properties.os AS \`$os\`,
  ev.data.properties.device AS \`$device_type\`
FROM {database}.events AS ev
WHERE ev.type IN ('page', 'track', 'identify')
`,
  down: `
DROP VIEW IF EXISTS posthog.events;
DROP VIEW IF EXISTS umami.website_events;
DROP VIEW IF EXISTS openpanel.events;
DROP VIEW IF EXISTS plausible.events
`.trim(),
}
