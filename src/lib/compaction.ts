/**
 * Event compaction logic — pure functions for SQL generation, cutoff computation,
 * and cooldown checks. The actual SQLite execution stays in DatabaseDO.
 */

export const DEFAULT_RETENTION_DAYS = 30
export const MIN_EVENTS_PER_ENTITY = 100
export const COMPACTION_WRITE_INTERVAL = 1000
export const COMPACTION_COOLDOWN_MS = 60 * 60 * 1000
export const COMPACTION_ALARM_INTERVAL_MS = 6 * 60 * 60 * 1000

export type CompactionResult = { deleted: number; compactedAt: string }

/**
 * Returns the DELETE SQL for event compaction.
 *
 * Placeholders:
 *   1. MIN_EVENTS_PER_ENTITY
 *   2. cutoff ISO timestamp
 */
export function buildCompactionQuery(): string {
  return `
      DELETE FROM events
      WHERE id IN (
        SELECT id FROM (
          SELECT id, timestamp,
                 ROW_NUMBER() OVER (
                   PARTITION BY entity_type, entity_id
                   ORDER BY timestamp DESC
                 ) as rn
          FROM events
        )
        WHERE rn > ? AND timestamp < ?
      )
    `
}

export function computeCompactionCutoff(retentionDays: number, now?: number): string {
  const ref = now ?? Date.now()
  return new Date(ref - retentionDays * 24 * 60 * 60 * 1000).toISOString()
}

export function shouldCompact(lastCompactionTimestamp: string | null, cooldownMs?: number, now?: number): boolean {
  if (lastCompactionTimestamp === null) return true
  const cooldown = cooldownMs ?? COMPACTION_COOLDOWN_MS
  const ref = now ?? Date.now()
  const elapsed = ref - new Date(lastCompactionTimestamp).getTime()
  return elapsed >= cooldown
}
