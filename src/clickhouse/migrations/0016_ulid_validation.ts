import type { Migration } from '../migrate'

/**
 * Migration 16: Delete events with non-ULID IDs.
 *
 * Entity IDs (contact_xxx, user_xxx, etc.) leaked into event IDs through
 * diagnostic endpoints that used entity.$id instead of ulid().
 *
 * Valid ULIDs are exactly 26 chars of Crockford Base32 [0-9A-HJKMNP-TV-Z].
 * This migration deletes rows that don't match.
 *
 * The S3Queue ingest MV (streams.ingest) is managed separately via
 * scripts/setup-s3queue.ts and should be recreated with ULID validation.
 */
export const migration: Migration = {
  version: 16,
  name: 'ulid_validation',
  up: `
ALTER TABLE {database}.events DELETE WHERE length(id) != 26 OR NOT match(id, '^[0-9A-HJKMNP-TV-Z]{26}$')
`,
  down: `SELECT 1`,
}
