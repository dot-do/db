import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 105,
  name: 'github',
  up: `
CREATE DATABASE IF NOT EXISTS github
;
CREATE OR REPLACE VIEW github.events AS
SELECT
  ev.data.deliveryId AS delivery_id,
  ev.data.eventType AS event_type,
  ev.data.payload.action AS action,
  ev.data.payload.sender.login AS sender,
  ev.data.payload.repository.full_name AS repository,
  ev.data.payload.organization.login AS org,
  ULIDStringToDateTime(ev.id) AS created_at,
  ev.data.verified AS verified,
  ev.data.payload AS payload,
  ev.ns AS ns,
  toString(ev.actor) AS actor
FROM {database}.events AS ev
WHERE ev.type = 'webhook' AND ev.data.provider = 'github'
;
CREATE OR REPLACE VIEW github.pushes AS
SELECT
  ev.data.payload.ref AS ref,
  ev.data.payload.before AS before_sha,
  ev.data.payload.after AS after_sha,
  ev.data.payload.pusher.name AS pusher,
  ev.data.payload.repository.full_name AS repository,
  ev.data.payload.forced AS forced,
  length(ev.data.payload.commits) AS commit_count,
  ULIDStringToDateTime(ev.id) AS created_at,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'webhook' AND ev.data.provider = 'github' AND ev.data.eventType = 'push'
;
CREATE OR REPLACE VIEW github.pull_requests AS
SELECT
  ev.data.payload.action AS action,
  ev.data.payload.pull_request.number AS number,
  ev.data.payload.pull_request.title AS title,
  ev.data.payload.pull_request.state AS state,
  ev.data.payload.pull_request.merged AS merged,
  ev.data.payload.pull_request.user.login AS author,
  ev.data.payload.pull_request.head.ref AS head_ref,
  ev.data.payload.pull_request.base.ref AS base_ref,
  ev.data.payload.repository.full_name AS repository,
  ULIDStringToDateTime(ev.id) AS created_at,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'webhook' AND ev.data.provider = 'github' AND ev.data.eventType = 'pull_request'
;
CREATE OR REPLACE VIEW github.issues AS
SELECT
  ev.data.payload.action AS action,
  ev.data.payload.issue.number AS number,
  ev.data.payload.issue.title AS title,
  ev.data.payload.issue.state AS state,
  ev.data.payload.issue.user.login AS author,
  ev.data.payload.issue.labels AS labels,
  ev.data.payload.repository.full_name AS repository,
  ULIDStringToDateTime(ev.id) AS created_at,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'webhook' AND ev.data.provider = 'github' AND ev.data.eventType = 'issues'
;
CREATE OR REPLACE VIEW github.deployments AS
SELECT
  ev.data.payload.action AS action,
  ev.data.payload.deployment.environment AS environment,
  ev.data.payload.deployment.ref AS ref,
  ev.data.payload.deployment.sha AS sha,
  ev.data.payload.deployment.task AS task,
  ev.data.payload.deployment.creator.login AS creator,
  ev.data.payload.repository.full_name AS repository,
  ULIDStringToDateTime(ev.id) AS created_at,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'webhook' AND ev.data.provider = 'github' AND ev.data.eventType LIKE 'deployment%'
;
CREATE OR REPLACE VIEW github.stars AS
SELECT
  ev.data.payload.action AS action,
  ev.data.payload.sender.login AS user,
  ev.data.payload.repository.full_name AS repository,
  ev.data.payload.repository.stargazers_count AS stargazers_count,
  ULIDStringToDateTime(ev.id) AS created_at,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'webhook' AND ev.data.provider = 'github' AND ev.data.eventType = 'star'
`,
  down: `
DROP VIEW IF EXISTS github.stars;
DROP VIEW IF EXISTS github.deployments;
DROP VIEW IF EXISTS github.issues;
DROP VIEW IF EXISTS github.pull_requests;
DROP VIEW IF EXISTS github.pushes;
DROP VIEW IF EXISTS github.events
`.trim(),
}
