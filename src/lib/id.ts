/**
 * Entity ID generation using sqids format.
 *
 * Every entity's $id follows the format: {type}_{sqid}
 * Examples: contact_Abc123, deal_Xz9kW, project_m4Qp7
 */

import Sqids from 'sqids'

const BLOCKED_WORDS = ['ass', 'damn', 'fuck', 'shit', 'hell', 'crap', 'dick', 'porn', 'sex', 'cum']

const sqids = new Sqids({
  minLength: 5,
  blocklist: new Set(BLOCKED_WORDS),
})

let counter = 0

/**
 * Generate a unique entity ID in the format {type}_{sqid}.
 */
export function generateEntityId(entityType: string): string {
  const prefix = entityType.toLowerCase()
  for (let attempt = 0; attempt < 10; attempt++) {
    const timestamp = Date.now()
    const random = Math.floor(Math.random() * 1000000)
    counter++
    const sqid = sqids.encode([timestamp, counter, random])
    const lower = sqid.toLowerCase()
    if (!BLOCKED_WORDS.some((w) => lower.includes(w))) {
      return `${prefix}_${sqid}`
    }
  }
  counter++
  return `${prefix}_${sqids.encode([Date.now(), counter, Math.floor(Math.random() * 1000000)])}`
}
