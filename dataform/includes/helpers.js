// Shared SQL snippets. Use in any .sqlx file as ${helpers.parseDatetime("column")}.

// The export writes timestamps in two formats:
//   2026-10-12 14:33:00   (most rows)
//   12-10-2026 14:33      (some rows in the 10-10 and 10-12 orders files)
// Try the first format; if it fails (SAFE. returns NULL), try the second.
// DATETIME, not TIMESTAMP: rule 7 says no timezone conversion.
function parseDatetime(column) {
  return `COALESCE(
    SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', TRIM(${column})),
    SAFE.PARSE_DATETIME('%d-%m-%Y %H:%M', TRIM(${column}))
  )`;
}

// Watermark for incremental tables: only read raw rows loaded after the newest row
// already in this table. COALESCE handles an existing-but-empty table.
function newRowsOnly(selfTable) {
  return `_loaded_at > (SELECT COALESCE(MAX(_loaded_at), TIMESTAMP '1970-01-01') FROM ${selfTable})`;
}

module.exports = { parseDatetime, newRowsOnly };
