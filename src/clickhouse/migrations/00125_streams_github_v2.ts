import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 125,
  name: 'streams_github_v2',
  up: `
DROP VIEW IF EXISTS streams.github_mv
;
DROP TABLE IF EXISTS streams.github
;
CREATE TABLE IF NOT EXISTS streams.github_pushes (
  id           String,
  ts           DateTime64(3),
  ns           String,
  delivery_id  String DEFAULT '',
  ref          String DEFAULT '',
  before       String DEFAULT '',
  after        String DEFAULT '',
  created      Bool DEFAULT false,
  deleted      Bool DEFAULT false,
  forced       Bool DEFAULT false,
  compare      String DEFAULT '',
  head_commit  JSON(max_dynamic_paths = 8),
  commits      String DEFAULT '[]',
  pusher       JSON(max_dynamic_paths = 4),
  repository   JSON(max_dynamic_paths = 16),
  sender       JSON(max_dynamic_paths = 8),
  organization JSON(max_dynamic_paths = 4)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_pushes_mv
TO streams.github_pushes AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.deliveryId, 'String') AS delivery_id,
  CAST(ev.data.\`payload.ref\`, 'String') AS ref,
  CAST(ev.data.\`payload.before\`, 'String') AS before,
  CAST(ev.data.\`payload.after\`, 'String') AS after,
  CAST(ev.data.\`payload.created\`, 'Bool') AS created,
  CAST(ev.data.\`payload.deleted\`, 'Bool') AS deleted,
  CAST(ev.data.\`payload.forced\`, 'Bool') AS forced,
  CAST(ev.data.\`payload.compare\`, 'String') AS compare,
  JSONExtractRaw(toString(ev.data), 'payload', 'head_commit') AS head_commit,
  JSONExtractRaw(toString(ev.data), 'payload', 'commits') AS commits,
  JSONExtractRaw(toString(ev.data), 'payload', 'pusher') AS pusher,
  JSONExtractRaw(toString(ev.data), 'payload', 'repository') AS repository,
  JSONExtractRaw(toString(ev.data), 'payload', 'sender') AS sender,
  JSONExtractRaw(toString(ev.data), 'payload', 'organization') AS organization
FROM {database}.events AS ev
WHERE ev.source = 'github' AND CAST(ev.data.eventType, 'String') = 'push'
;

CREATE TABLE IF NOT EXISTS streams.github_pull_requests (
  id           String,
  ts           DateTime64(3),
  ns           String,
  delivery_id  String DEFAULT '',
  action       LowCardinality(String) DEFAULT '',
  number       UInt32 DEFAULT 0,
  title        String DEFAULT '',
  state        LowCardinality(String) DEFAULT '',
  merged       Bool DEFAULT false,
  draft        Bool DEFAULT false,
  repo         LowCardinality(String) DEFAULT '',
  pull_request JSON(max_dynamic_paths = 32),
  repository   JSON(max_dynamic_paths = 16),
  sender       JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_pull_requests_mv
TO streams.github_pull_requests AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.deliveryId, 'String') AS delivery_id,
  CAST(ev.data.\`payload.action\`, 'String') AS action,
  CAST(ev.data.\`payload.number\`, 'UInt32') AS number,
  CAST(ev.data.\`payload.pull_request.title\`, 'String') AS title,
  CAST(ev.data.\`payload.pull_request.state\`, 'String') AS state,
  CAST(ev.data.\`payload.pull_request.merged\`, 'Bool') AS merged,
  CAST(ev.data.\`payload.pull_request.draft\`, 'Bool') AS draft,
  CAST(ev.data.\`payload.repository.full_name\`, 'String') AS repo,
  JSONExtractRaw(toString(ev.data), 'payload', 'pull_request') AS pull_request,
  JSONExtractRaw(toString(ev.data), 'payload', 'repository') AS repository,
  JSONExtractRaw(toString(ev.data), 'payload', 'sender') AS sender
FROM {database}.events AS ev
WHERE ev.source = 'github' AND CAST(ev.data.eventType, 'String') = 'pull_request'
;

CREATE TABLE IF NOT EXISTS streams.github_issues (
  id          String,
  ts          DateTime64(3),
  ns          String,
  delivery_id String DEFAULT '',
  action      LowCardinality(String) DEFAULT '',
  number      UInt32 DEFAULT 0,
  title       String DEFAULT '',
  state       LowCardinality(String) DEFAULT '',
  repo        LowCardinality(String) DEFAULT '',
  issue       JSON(max_dynamic_paths = 16),
  repository  JSON(max_dynamic_paths = 16),
  sender      JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_issues_mv
TO streams.github_issues AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.deliveryId, 'String') AS delivery_id,
  CAST(ev.data.\`payload.action\`, 'String') AS action,
  CAST(ev.data.\`payload.issue.number\`, 'UInt32') AS number,
  CAST(ev.data.\`payload.issue.title\`, 'String') AS title,
  CAST(ev.data.\`payload.issue.state\`, 'String') AS state,
  CAST(ev.data.\`payload.repository.full_name\`, 'String') AS repo,
  JSONExtractRaw(toString(ev.data), 'payload', 'issue') AS issue,
  JSONExtractRaw(toString(ev.data), 'payload', 'repository') AS repository,
  JSONExtractRaw(toString(ev.data), 'payload', 'sender') AS sender
FROM {database}.events AS ev
WHERE ev.source = 'github' AND CAST(ev.data.eventType, 'String') = 'issues'
;

CREATE TABLE IF NOT EXISTS streams.github_issue_comments (
  id           String,
  ts           DateTime64(3),
  ns           String,
  delivery_id  String DEFAULT '',
  action       LowCardinality(String) DEFAULT '',
  issue_number UInt32 DEFAULT 0,
  repo         LowCardinality(String) DEFAULT '',
  comment      JSON(max_dynamic_paths = 8),
  issue        JSON(max_dynamic_paths = 16),
  repository   JSON(max_dynamic_paths = 16),
  sender       JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_issue_comments_mv
TO streams.github_issue_comments AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.deliveryId, 'String') AS delivery_id,
  CAST(ev.data.\`payload.action\`, 'String') AS action,
  CAST(ev.data.\`payload.issue.number\`, 'UInt32') AS issue_number,
  CAST(ev.data.\`payload.repository.full_name\`, 'String') AS repo,
  JSONExtractRaw(toString(ev.data), 'payload', 'comment') AS comment,
  JSONExtractRaw(toString(ev.data), 'payload', 'issue') AS issue,
  JSONExtractRaw(toString(ev.data), 'payload', 'repository') AS repository,
  JSONExtractRaw(toString(ev.data), 'payload', 'sender') AS sender
FROM {database}.events AS ev
WHERE ev.source = 'github' AND CAST(ev.data.eventType, 'String') = 'issue_comment'
;

CREATE TABLE IF NOT EXISTS streams.github_workflow_runs (
  id           String,
  ts           DateTime64(3),
  ns           String,
  delivery_id  String DEFAULT '',
  action       LowCardinality(String) DEFAULT '',
  workflow_id  UInt64 DEFAULT 0,
  name         String DEFAULT '',
  run_number   UInt32 DEFAULT 0,
  status       LowCardinality(String) DEFAULT '',
  conclusion   LowCardinality(String) DEFAULT '',
  head_branch  String DEFAULT '',
  head_sha     String DEFAULT '',
  event        LowCardinality(String) DEFAULT '',
  repo         LowCardinality(String) DEFAULT '',
  workflow_run JSON(max_dynamic_paths = 16),
  workflow     JSON(max_dynamic_paths = 8),
  repository   JSON(max_dynamic_paths = 16),
  sender       JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_workflow_runs_mv
TO streams.github_workflow_runs AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.deliveryId, 'String') AS delivery_id,
  CAST(ev.data.\`payload.action\`, 'String') AS action,
  CAST(ev.data.\`payload.workflow_run.workflow_id\`, 'UInt64') AS workflow_id,
  CAST(ev.data.\`payload.workflow_run.name\`, 'String') AS name,
  CAST(ev.data.\`payload.workflow_run.run_number\`, 'UInt32') AS run_number,
  CAST(ev.data.\`payload.workflow_run.status\`, 'String') AS status,
  CAST(ev.data.\`payload.workflow_run.conclusion\`, 'String') AS conclusion,
  CAST(ev.data.\`payload.workflow_run.head_branch\`, 'String') AS head_branch,
  CAST(ev.data.\`payload.workflow_run.head_sha\`, 'String') AS head_sha,
  CAST(ev.data.\`payload.workflow_run.event\`, 'String') AS event,
  CAST(ev.data.\`payload.repository.full_name\`, 'String') AS repo,
  JSONExtractRaw(toString(ev.data), 'payload', 'workflow_run') AS workflow_run,
  JSONExtractRaw(toString(ev.data), 'payload', 'workflow') AS workflow,
  JSONExtractRaw(toString(ev.data), 'payload', 'repository') AS repository,
  JSONExtractRaw(toString(ev.data), 'payload', 'sender') AS sender
FROM {database}.events AS ev
WHERE ev.source = 'github' AND CAST(ev.data.eventType, 'String') = 'workflow_run'
;

CREATE TABLE IF NOT EXISTS streams.github_workflow_jobs (
  id           String,
  ts           DateTime64(3),
  ns           String,
  delivery_id  String DEFAULT '',
  action       LowCardinality(String) DEFAULT '',
  run_id       UInt64 DEFAULT 0,
  name         String DEFAULT '',
  status       LowCardinality(String) DEFAULT '',
  conclusion   LowCardinality(String) DEFAULT '',
  started_at   String DEFAULT '',
  completed_at String DEFAULT '',
  runner_name  String DEFAULT '',
  repo         LowCardinality(String) DEFAULT '',
  workflow_job JSON(max_dynamic_paths = 16),
  repository   JSON(max_dynamic_paths = 16),
  sender       JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_workflow_jobs_mv
TO streams.github_workflow_jobs AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.deliveryId, 'String') AS delivery_id,
  CAST(ev.data.\`payload.action\`, 'String') AS action,
  CAST(ev.data.\`payload.workflow_job.run_id\`, 'UInt64') AS run_id,
  CAST(ev.data.\`payload.workflow_job.name\`, 'String') AS name,
  CAST(ev.data.\`payload.workflow_job.status\`, 'String') AS status,
  CAST(ev.data.\`payload.workflow_job.conclusion\`, 'String') AS conclusion,
  CAST(ev.data.\`payload.workflow_job.started_at\`, 'String') AS started_at,
  CAST(ev.data.\`payload.workflow_job.completed_at\`, 'String') AS completed_at,
  CAST(ev.data.\`payload.workflow_job.runner_name\`, 'String') AS runner_name,
  CAST(ev.data.\`payload.repository.full_name\`, 'String') AS repo,
  JSONExtractRaw(toString(ev.data), 'payload', 'workflow_job') AS workflow_job,
  JSONExtractRaw(toString(ev.data), 'payload', 'repository') AS repository,
  JSONExtractRaw(toString(ev.data), 'payload', 'sender') AS sender
FROM {database}.events AS ev
WHERE ev.source = 'github' AND CAST(ev.data.eventType, 'String') = 'workflow_job'
;

CREATE TABLE IF NOT EXISTS streams.github_check_runs (
  id          String,
  ts          DateTime64(3),
  ns          String,
  delivery_id String DEFAULT '',
  action      LowCardinality(String) DEFAULT '',
  name        String DEFAULT '',
  status      LowCardinality(String) DEFAULT '',
  conclusion  LowCardinality(String) DEFAULT '',
  head_sha    String DEFAULT '',
  repo        LowCardinality(String) DEFAULT '',
  check_run   JSON(max_dynamic_paths = 16),
  repository  JSON(max_dynamic_paths = 16),
  sender      JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_check_runs_mv
TO streams.github_check_runs AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.deliveryId, 'String') AS delivery_id,
  CAST(ev.data.\`payload.action\`, 'String') AS action,
  CAST(ev.data.\`payload.check_run.name\`, 'String') AS name,
  CAST(ev.data.\`payload.check_run.status\`, 'String') AS status,
  CAST(ev.data.\`payload.check_run.conclusion\`, 'String') AS conclusion,
  CAST(ev.data.\`payload.check_run.head_sha\`, 'String') AS head_sha,
  CAST(ev.data.\`payload.repository.full_name\`, 'String') AS repo,
  JSONExtractRaw(toString(ev.data), 'payload', 'check_run') AS check_run,
  JSONExtractRaw(toString(ev.data), 'payload', 'repository') AS repository,
  JSONExtractRaw(toString(ev.data), 'payload', 'sender') AS sender
FROM {database}.events AS ev
WHERE ev.source = 'github' AND CAST(ev.data.eventType, 'String') = 'check_run'
;

CREATE TABLE IF NOT EXISTS streams.github_check_suites (
  id          String,
  ts          DateTime64(3),
  ns          String,
  delivery_id String DEFAULT '',
  action      LowCardinality(String) DEFAULT '',
  head_branch String DEFAULT '',
  head_sha    String DEFAULT '',
  status      LowCardinality(String) DEFAULT '',
  conclusion  LowCardinality(String) DEFAULT '',
  repo        LowCardinality(String) DEFAULT '',
  check_suite JSON(max_dynamic_paths = 16),
  repository  JSON(max_dynamic_paths = 16),
  sender      JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_check_suites_mv
TO streams.github_check_suites AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.deliveryId, 'String') AS delivery_id,
  CAST(ev.data.\`payload.action\`, 'String') AS action,
  CAST(ev.data.\`payload.check_suite.head_branch\`, 'String') AS head_branch,
  CAST(ev.data.\`payload.check_suite.head_sha\`, 'String') AS head_sha,
  CAST(ev.data.\`payload.check_suite.status\`, 'String') AS status,
  CAST(ev.data.\`payload.check_suite.conclusion\`, 'String') AS conclusion,
  CAST(ev.data.\`payload.repository.full_name\`, 'String') AS repo,
  JSONExtractRaw(toString(ev.data), 'payload', 'check_suite') AS check_suite,
  JSONExtractRaw(toString(ev.data), 'payload', 'repository') AS repository,
  JSONExtractRaw(toString(ev.data), 'payload', 'sender') AS sender
FROM {database}.events AS ev
WHERE ev.source = 'github' AND CAST(ev.data.eventType, 'String') = 'check_suite'
;

CREATE TABLE IF NOT EXISTS streams.github_deployments (
  id          String,
  ts          DateTime64(3),
  ns          String,
  delivery_id String DEFAULT '',
  action      LowCardinality(String) DEFAULT '',
  environment LowCardinality(String) DEFAULT '',
  task        String DEFAULT '',
  ref         String DEFAULT '',
  description String DEFAULT '',
  repo        LowCardinality(String) DEFAULT '',
  deployment  JSON(max_dynamic_paths = 8),
  repository  JSON(max_dynamic_paths = 16),
  sender      JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_deployments_mv
TO streams.github_deployments AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.deliveryId, 'String') AS delivery_id,
  CAST(ev.data.\`payload.action\`, 'String') AS action,
  coalesce(
    nullIf(CAST(ev.data.\`payload.deployment_status.environment\`, 'String'), ''),
    CAST(ev.data.\`payload.deployment.environment\`, 'String')
  ) AS environment,
  CAST(ev.data.\`payload.deployment.task\`, 'String') AS task,
  CAST(ev.data.\`payload.deployment.ref\`, 'String') AS ref,
  CAST(ev.data.\`payload.deployment.description\`, 'String') AS description,
  CAST(ev.data.\`payload.repository.full_name\`, 'String') AS repo,
  if(
    CAST(ev.data.eventType, 'String') = 'deployment_status',
    JSONExtractRaw(toString(ev.data), 'payload', 'deployment_status'),
    JSONExtractRaw(toString(ev.data), 'payload', 'deployment')
  ) AS deployment,
  JSONExtractRaw(toString(ev.data), 'payload', 'repository') AS repository,
  JSONExtractRaw(toString(ev.data), 'payload', 'sender') AS sender
FROM {database}.events AS ev
WHERE ev.source = 'github' AND CAST(ev.data.eventType, 'String') IN ('deployment', 'deployment_status')
;

CREATE TABLE IF NOT EXISTS streams.github_security (
  id                String,
  ts                DateTime64(3),
  ns                String,
  delivery_id       String DEFAULT '',
  action            LowCardinality(String) DEFAULT '',
  severity          LowCardinality(String) DEFAULT '',
  state             LowCardinality(String) DEFAULT '',
  package_name      String DEFAULT '',
  package_ecosystem String DEFAULT '',
  repo              LowCardinality(String) DEFAULT '',
  alert             JSON(max_dynamic_paths = 16),
  advisory          JSON(max_dynamic_paths = 8),
  repository        JSON(max_dynamic_paths = 16),
  sender            JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_security_mv
TO streams.github_security AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.deliveryId, 'String') AS delivery_id,
  CAST(ev.data.\`payload.action\`, 'String') AS action,
  coalesce(
    nullIf(CAST(ev.data.\`payload.alert.security_advisory.severity\`, 'String'), ''),
    nullIf(CAST(ev.data.\`payload.security_advisory.severity\`, 'String'), '')
  ) AS severity,
  CAST(ev.data.\`payload.alert.state\`, 'String') AS state,
  CAST(ev.data.\`payload.alert.security_vulnerability.package.name\`, 'String') AS package_name,
  CAST(ev.data.\`payload.alert.security_vulnerability.package.ecosystem\`, 'String') AS package_ecosystem,
  CAST(ev.data.\`payload.repository.full_name\`, 'String') AS repo,
  coalesce(
    nullIf(JSONExtractRaw(toString(ev.data), 'payload', 'alert'), ''),
    JSONExtractRaw(toString(ev.data), 'payload', 'security_advisory')
  ) AS alert,
  JSONExtractRaw(toString(ev.data), 'payload', 'security_advisory') AS advisory,
  JSONExtractRaw(toString(ev.data), 'payload', 'repository') AS repository,
  JSONExtractRaw(toString(ev.data), 'payload', 'sender') AS sender
FROM {database}.events AS ev
WHERE ev.source = 'github'
  AND CAST(ev.data.eventType, 'String') IN ('security_advisory', 'dependabot_alert', 'repository_vulnerability_alert')
`.trim(),
  down: `
DROP VIEW IF EXISTS streams.github_security_mv
;
DROP VIEW IF EXISTS streams.github_deployments_mv
;
DROP VIEW IF EXISTS streams.github_check_suites_mv
;
DROP VIEW IF EXISTS streams.github_check_runs_mv
;
DROP VIEW IF EXISTS streams.github_workflow_jobs_mv
;
DROP VIEW IF EXISTS streams.github_workflow_runs_mv
;
DROP VIEW IF EXISTS streams.github_issue_comments_mv
;
DROP VIEW IF EXISTS streams.github_issues_mv
;
DROP VIEW IF EXISTS streams.github_pull_requests_mv
;
DROP VIEW IF EXISTS streams.github_pushes_mv
;
DROP TABLE IF EXISTS streams.github_security
;
DROP TABLE IF EXISTS streams.github_deployments
;
DROP TABLE IF EXISTS streams.github_check_suites
;
DROP TABLE IF EXISTS streams.github_check_runs
;
DROP TABLE IF EXISTS streams.github_workflow_jobs
;
DROP TABLE IF EXISTS streams.github_workflow_runs
;
DROP TABLE IF EXISTS streams.github_issue_comments
;
DROP TABLE IF EXISTS streams.github_issues
;
DROP TABLE IF EXISTS streams.github_pull_requests
;
DROP TABLE IF EXISTS streams.github_pushes
;
CREATE TABLE IF NOT EXISTS streams.github (
  entity_type  LowCardinality(String),
  entity_id    String,
  action       LowCardinality(String),
  ns           String,
  ts           DateTime64(3),
  event_id     String,
  event_type   LowCardinality(String),
  sender       String DEFAULT '',
  payload      String,
  version      UInt64
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(ts)
ORDER BY (entity_type, entity_id)
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_mv
TO streams.github AS
SELECT
  ev.data.entity::String AS entity_type,
  ev.data.id::String AS entity_id,
  ev.data.action::String AS action,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.id AS event_id,
  ev.data.eventType::String AS event_type,
  ev.actor.id::String AS sender,
  toString(ev.data.payload) AS payload,
  toUnixTimestamp64Milli(ULIDStringToDateTime(ev.id)) AS version
FROM {database}.events AS ev
WHERE ev.source = 'github'
`.trim(),
}
