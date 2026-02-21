/**
 * CDC Event Forwarding via Pipeline
 *
 * Fire-and-forget forwarding of CDC events to the EVENTS_PIPELINE binding.
 * Falls back to service binding HTTP if Pipeline is not available.
 */

export const OPERATION_TYPE_MAP: Record<string, string> = {
  create: 'collection.insert',
  update: 'collection.update',
  delete: 'collection.delete',
}

export function mapOperationToEventType(operation: string): string {
  return OPERATION_TYPE_MAP[operation] ?? `collection.${operation}`
}

export interface DurableEvent {
  type: string
  ts: string
  do: { id: string; class: string }
  collection: string
  docId: string
  doc?: Record<string, unknown>
  prev?: Record<string, unknown>
}

export interface PipelineLike {
  send(records: Record<string, unknown>[]): Promise<void>
}

export interface EventsBinding {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>
}

export function forwardEvents(binding: PipelineLike | EventsBinding, events: DurableEvent[]): Promise<void> {
  if (events.length === 0) return Promise.resolve()

  // Pipeline binding (preferred)
  if ('send' in binding) {
    return binding.send(events as unknown as Record<string, unknown>[]).catch((err: unknown) => {
      console.warn('[EventForwarding] Pipeline send failed:', err)
    })
  }

  // Fallback: service binding HTTP
  return binding
    .fetch('https://events.do/ingest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events }),
    })
    .then(() => {})
    .catch((err: unknown) => {
      console.warn('[EventForwarding] EVENTS forwarding failed:', err)
    })
}

export function buildDurableEvent(
  operation: string,
  collection: string,
  docId: string,
  doIdentity: { id: string; class: string },
  doc?: Record<string, unknown>,
  prev?: Record<string, unknown>,
): DurableEvent {
  const event: DurableEvent = {
    type: mapOperationToEventType(operation),
    ts: new Date().toISOString(),
    do: doIdentity,
    collection,
    docId,
  }
  if (doc) event.doc = doc
  if (prev) event.prev = prev
  return event
}
