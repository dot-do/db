import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 100,
  name: 'segment',
  up: `
CREATE DATABASE IF NOT EXISTS segment
;
CREATE VIEW IF NOT EXISTS segment.events AS
SELECT
  ev.id AS message_id,
  ev.type AS type,
  ev.event AS event,
  ev.actor AS user_id,
  ev.data.anonymousId AS anonymous_id,
  ev.data.sessionId AS session_id,
  ev.data.groupId AS group_id,
  ev.data.previousId AS previous_id,
  ev.ts AS timestamp,
  ev.ts AS received_at,
  ev.url AS url,
  ev.source AS channel,
  toString(ev.data.properties) AS properties,
  toString(ev.data.traits) AS traits,
  toString(ev.data.context) AS context,
  ev.data.name AS name,
  ev.data.path AS path,
  ev.data.referrer AS referrer,
  ev.data.category AS category,
  ev.ns AS namespace
FROM {database}.events AS ev
WHERE ev.type IN ('page', 'track', 'identify', 'screen', 'group', 'alias')
;
CREATE VIEW IF NOT EXISTS segment.tracks AS
SELECT * FROM segment.events WHERE type = 'track'
;
CREATE VIEW IF NOT EXISTS segment.identifies AS
SELECT * FROM segment.events WHERE type = 'identify'
;
CREATE VIEW IF NOT EXISTS segment.pages AS
SELECT * FROM segment.events WHERE type = 'page'
;
CREATE VIEW IF NOT EXISTS segment.screens AS
SELECT * FROM segment.events WHERE type = 'screen'
;
CREATE VIEW IF NOT EXISTS segment.groups AS
SELECT * FROM segment.events WHERE type = 'group'
;
CREATE VIEW IF NOT EXISTS segment.aliases AS
SELECT * FROM segment.events WHERE type = 'alias'
;
CREATE VIEW IF NOT EXISTS segment.users AS
SELECT
  ev.actor AS id,
  max(ev.ts) AS received_at,
  argMax(ev.data.traits.email, ev.ts) AS email,
  argMax(ev.data.traits.name, ev.ts) AS name,
  argMax(ev.data.traits, ev.ts) AS traits,
  ev.ns AS namespace
FROM {database}.events AS ev
WHERE ev.type = 'identify' AND ev.actor != ''
GROUP BY ev.actor, ev.ns
`,
  down: `
DROP VIEW IF EXISTS segment.users;
DROP VIEW IF EXISTS segment.aliases;
DROP VIEW IF EXISTS segment.groups;
DROP VIEW IF EXISTS segment.screens;
DROP VIEW IF EXISTS segment.pages;
DROP VIEW IF EXISTS segment.identifies;
DROP VIEW IF EXISTS segment.tracks;
DROP VIEW IF EXISTS segment.events
`.trim(),
}
