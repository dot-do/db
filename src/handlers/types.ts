/**
 * Shared types for DatabaseDO handler modules.
 */

export interface Entity {
  $id?: string
  $type?: string
  $version?: number
  $createdAt?: string
  $updatedAt?: string
  $createdBy?: string
  $updatedBy?: string
  $deletedAt?: string
  [key: string]: unknown
}

export interface SyncEvent {
  id: string
  timestamp: string
  operation: 'create' | 'update' | 'delete' | 'soft-delete'
  type: string
  entityId: string
  data?: Record<string, unknown>
  checksum?: string
}

export interface SyncRequest {
  events?: SyncEvent[]
  strategy?: 'local-wins' | 'remote-wins' | 'newest'
  since?: string
  cursor?: string
  limit?: number
}

export interface SyncConflict {
  entityId: string
  type: string
  localVersion: string
  remoteVersion: string
  resolution?: 'local' | 'remote' | 'pending'
}
