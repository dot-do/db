/**
 * Purge Policy — permanent removal of expired soft-deleted entities.
 *
 * Default retention: 90 days from $deletedAt.
 */

export const DEFAULT_PURGE_RETENTION_DAYS = 90

export interface PurgeContext {
  entityStore: Map<string, Record<string, unknown>>
  getCollection: (type: string) => { delete(id: string): Promise<unknown> }
  safeLogEvent: (operation: string, type: string, entityId: string, data?: Record<string, unknown>) => string | undefined
  getEntityTypes: () => string[]
}

export interface PurgeOptions {
  retentionDays?: number
}

export interface PurgeResult {
  purged: number
  purgedEntities: Array<{ entityType: string; entityId: string }>
  purgedAt: string
}

export interface PreviewResult {
  wouldPurge: number
  entities: Array<Record<string, unknown>>
  byType: Record<string, number>
}

function isExpired(deletedAt: string, retentionDays: number): boolean {
  const deletedDate = new Date(deletedAt)
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - retentionDays)
  return deletedDate < cutoff
}

function findExpiredEntities(
  entityStore: Map<string, Record<string, unknown>>,
  retentionDays: number,
): Array<{ key: string; entityType: string; entityId: string; entity: Record<string, unknown> }> {
  const expired: Array<{ key: string; entityType: string; entityId: string; entity: Record<string, unknown> }> = []

  for (const [key, entity] of entityStore) {
    if (!entity.$deletedAt || typeof entity.$deletedAt !== 'string') continue
    if (!isExpired(entity.$deletedAt, retentionDays)) continue

    const [entityType, entityId] = key.split(':')
    expired.push({ key, entityType, entityId, entity })
  }

  return expired
}

export async function purgeExpiredEntities(ctx: PurgeContext, options: PurgeOptions = {}): Promise<PurgeResult> {
  const retentionDays = options.retentionDays ?? DEFAULT_PURGE_RETENTION_DAYS
  const purgedAt = new Date().toISOString()
  const expired = findExpiredEntities(ctx.entityStore, retentionDays)

  const purgedEntities: Array<{ entityType: string; entityId: string }> = []

  for (const { key, entityType, entityId, entity } of expired) {
    ctx.safeLogEvent('purge', entityType, entityId, { ...entity })
    ctx.entityStore.delete(key)
    purgedEntities.push({ entityType, entityId })
  }

  return { purged: purgedEntities.length, purgedEntities, purgedAt }
}

export async function previewPurge(ctx: PurgeContext, options: PurgeOptions = {}): Promise<PreviewResult> {
  const retentionDays = options.retentionDays ?? DEFAULT_PURGE_RETENTION_DAYS
  const expired = findExpiredEntities(ctx.entityStore, retentionDays)

  const byType: Record<string, number> = {}
  for (const { entityType } of expired) {
    byType[entityType] = (byType[entityType] ?? 0) + 1
  }

  return {
    wouldPurge: expired.length,
    entities: expired.map(({ entity }) => entity),
    byType,
  }
}
