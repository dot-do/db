/**
 * Time Travel — Entity Reconstruction from CDC Events
 *
 * Pure function that replays chronological CDC events to reconstruct
 * an entity's state at a given point in time.
 */

export type EventRow = {
  operation: string
  data?: Record<string, unknown>
  timestamp: string
}

export type ReconstructionResult = {
  entity: Record<string, unknown> | null
  deleted: boolean
  deletedAt?: string
}

/**
 * Replay a chronologically-sorted array of CDC events to reconstruct entity state.
 * Events MUST already be filtered to the target entity and sorted by timestamp ASC.
 */
export function reconstructFromEvents(events: EventRow[]): ReconstructionResult {
  if (events.length === 0) {
    return { entity: null, deleted: false }
  }

  let state: Record<string, unknown> | null = null
  let deleted = false
  let deletedAt: string | undefined

  for (const event of events) {
    const { operation, data, timestamp } = event

    switch (operation) {
      case 'create':
        state = data ? { ...data } : null
        deleted = false
        deletedAt = undefined
        break
      case 'update':
        if (state && data) {
          state = { ...data }
        }
        break
      case 'soft-delete':
        deleted = true
        deletedAt = timestamp
        if (data) {
          state = { ...data }
        }
        break
      default:
        if (state && data) {
          state = { ...data }
        }
        break
    }
  }

  return { entity: state, deleted, ...(deletedAt !== undefined ? { deletedAt } : {}) }
}
