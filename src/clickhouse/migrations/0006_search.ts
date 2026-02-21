import type { Migration } from '../migrate'

// TODO: Add vector_similarity index once embedding dimension is known.
// ClickHouse 25.10 requires a fixed dimension > 0 for vector_similarity indexes:
//   INDEX vec_idx(embedding) TYPE vector_similarity('hnsw', 'cosineDistance', <dim>)
// This must be added via a later migration after the embedding model is chosen.

export const migration: Migration = {
  version: 6,
  name: 'search',
  up: `
CREATE TABLE IF NOT EXISTS {database}.search (
  url         String,
  ns          String,
  type        String,
  id          String,
  name        Nullable(String),
  content     String,
  embedding   Array(Float32),
  visibility  Nullable(String),
  v           UInt64,
  INDEX text_idx(content) TYPE text(tokenizer = 'splitByNonAlpha')
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, type, id)
SETTINGS allow_experimental_full_text_index = 1
`,
  down: 'DROP TABLE IF EXISTS {database}.search',
}
