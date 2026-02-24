/**
 * Core entity types for the DB Durable Object.
 * Extracted to a standalone module to avoid circular imports.
 */

/**
 * Flat entity shape (mdxld convention).
 * Meta fields are $-prefixed and mixed with business data.
 * This is the canonical return type for all DB RPC methods.
 */
export interface DBEntity {
  $id: string
  $type: string
  $version: number
  $createdAt: string
  $updatedAt: string
  $createdBy?: string
  $updatedBy?: string
  [key: string]: unknown
}

/**
 * Expanded entity shape — meta separated from data.
 * Useful for storage-oriented consumers that prefer { id, type, data }.
 */
export interface DBEntityExpanded {
  id: string
  type: string
  version: number
  createdAt: string
  updatedAt: string
  createdBy?: string
  updatedBy?: string
  data: Record<string, unknown>
}

export interface FindResult {
  items: DBEntity[]
  total: number
  hasMore: boolean
}

export interface DeleteResult {
  deletedCount: number
}

/** Convert flat entity to expanded shape */
export function toExpanded(entity: DBEntity): DBEntityExpanded {
  const { $id, $type, $version, $createdAt, $updatedAt, $createdBy, $updatedBy, ...data } = entity
  return {
    id: $id,
    type: $type,
    version: $version,
    createdAt: $createdAt,
    updatedAt: $updatedAt,
    ...$createdBy ? { createdBy: $createdBy } : {},
    ...$updatedBy ? { updatedBy: $updatedBy } : {},
    data,
  }
}

/** Convert expanded shape to flat entity */
export function toFlat(expanded: DBEntityExpanded): DBEntity {
  return {
    $id: expanded.id,
    $type: expanded.type,
    $version: expanded.version,
    $createdAt: expanded.createdAt,
    $updatedAt: expanded.updatedAt,
    ...expanded.createdBy ? { $createdBy: expanded.createdBy } : {},
    ...expanded.updatedBy ? { $updatedBy: expanded.updatedBy } : {},
    ...expanded.data,
  }
}
