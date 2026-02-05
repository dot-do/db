/**
 * Storage Backend Helpers
 *
 * Convenience functions for creating storage backends.
 *
 * @example
 * ```typescript
 * import { createLocalBackend, createMemoryBackend } from '@dotdo/db/backends'
 *
 * // Local filesystem backend
 * const local = createLocalBackend('.db')
 *
 * // In-memory backend for testing
 * const memory = createMemoryBackend()
 * ```
 *
 * @packageDocumentation
 */

import { FsBackend, MemoryBackend, R2Backend } from 'parquedb'
import type { R2BackendOptions } from 'parquedb'

/**
 * Create a local filesystem storage backend.
 *
 * @param basePath - Base directory for storage (default: '.db')
 * @returns FsBackend instance
 *
 * @example
 * ```typescript
 * import { DB } from '@dotdo/db'
 * import { createLocalBackend } from '@dotdo/db/backends'
 *
 * const db = DB({ schema: 'flexible' }, {
 *   storage: createLocalBackend('./data')
 * })
 * ```
 */
export function createLocalBackend(basePath: string = '.db'): FsBackend {
  return new FsBackend({ basePath })
}

/**
 * Create an in-memory storage backend.
 *
 * Useful for testing, prototyping, or ephemeral data.
 * Data does not persist across restarts.
 *
 * @returns MemoryBackend instance
 *
 * @example
 * ```typescript
 * import { DB } from '@dotdo/db'
 * import { createMemoryBackend } from '@dotdo/db/backends'
 *
 * const testDB = DB({ schema: 'flexible' }, {
 *   storage: createMemoryBackend()
 * })
 * ```
 */
export function createMemoryBackend(): MemoryBackend {
  return new MemoryBackend()
}

/**
 * Create a Cloudflare R2 storage backend.
 *
 * For use in Cloudflare Workers with R2 bucket binding.
 *
 * @param bucket - R2 bucket binding from env
 * @param options - Optional R2 backend configuration
 * @returns R2Backend instance
 *
 * @example
 * ```typescript
 * import { DB } from '@dotdo/db'
 * import { createR2Backend } from '@dotdo/db/backends'
 *
 * export default {
 *   async fetch(request, env) {
 *     const db = DB({ schema: 'flexible' }, {
 *       storage: createR2Backend(env.MY_BUCKET)
 *     })
 *     // ...
 *   }
 * }
 * ```
 */
export function createR2Backend(
  bucket: R2Bucket,
  options?: Omit<R2BackendOptions, 'bucket'>
): R2Backend {
  return new R2Backend({ ...options, bucket } as R2BackendOptions)
}

/**
 * R2Bucket type for Cloudflare Workers
 * Re-exported for convenience
 */
export type { R2Bucket } from '@cloudflare/workers-types'

// Re-export backend classes for direct instantiation
export { FsBackend, MemoryBackend, R2Backend }
