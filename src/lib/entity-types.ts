/**
 * Entity type name utilities.
 *
 * Converts collection names to PascalCase entity types, validates names
 * against prototype-pollution attacks, and provides both throwing and
 * non-throwing validation helpers.
 */

/** Property names that must never be used as entity type names */
export const DANGEROUS_TYPE_NAMES = new Set(['__proto__', 'constructor', 'prototype', 'toString', 'valueOf'])

/**
 * Singularize a collection name back to PascalCase entity type.
 * e.g., 'contacts' -> 'Contact', 'companies' -> 'Company', 'featureFlags' -> 'FeatureFlag'
 */
export function toEntityType(collectionName: string): string {
  const parts = collectionName
    .replace(/([A-Z])/g, ' $1')
    .trim()
    .split(' ')
  const last = parts[parts.length - 1]

  let singular: string
  if (last.endsWith('ies')) {
    singular = last.slice(0, -3) + 'y'
  } else if (last.endsWith('ses') || last.endsWith('xes') || last.endsWith('ches') || last.endsWith('shes')) {
    singular = last.slice(0, -2)
  } else if (last.endsWith('s') && !last.endsWith('ss')) {
    singular = last.slice(0, -1)
  } else {
    singular = last
  }

  parts[parts.length - 1] = singular
  return parts.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join('')
}

/**
 * Validate that an entity type name is safe to use as a property accessor.
 * Throws on empty or dangerous names.
 */
export function validateEntityType(type: string): void {
  if (!type || typeof type !== 'string') {
    throw new Error('Entity type must be a non-empty string')
  }
  if (DANGEROUS_TYPE_NAMES.has(type)) {
    throw new Error(`Invalid entity type: '${type}' is a reserved name`)
  }
}

/**
 * Non-throwing version of validateEntityType.
 */
export function isValidEntityType(type: string): boolean {
  if (!type || typeof type !== 'string') return false
  return !DANGEROUS_TYPE_NAMES.has(type)
}
