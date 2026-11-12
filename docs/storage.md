# Case storage

Each saved case is a portable `projects/<uuid>/` directory containing immutable
`data.parquet` and one canonical `project.sqlite` database. Polars continues to
query and aggregate Parquet. SQLite stores project metadata, annotations,
annotation counts and revisions, view and column settings, IOC definitions, and
one durable bulk undo operation. The workspace project list is discovered from
case metadata; it does not maintain a second writable registry.

SQLite uses WAL transactions. A single annotation edit or bulk operation commits
its annotations, flagged count, metadata count, revision and undo changes together.
Cancellation or a write failure rolls back the whole operation. Queries can read
annotations and their revision from one consistent snapshot. Connections belong
to individual operations and use a 2 MiB page cache; inactive tabs do not retain
SQLite connections. Recognized Safe/Suspicious/Critical labels and their legacy
aliases contribute to flagged counts. Unknown legacy text stays stored and is
treated as Unflagged, consistently with query and timeline filters.

Bulk undo streams previous/applied values through SQLite rather than retaining
whole annotation vectors in memory. Undo survives an application restart. It
refuses to overwrite subsequent edits to affected rows, while preserving edits
to unrelated rows. There is one undo operation per case. The serialized payload
limit defaults to 512 MiB; the positive integer project setting
`bulk_undo_limit_bytes` can override it. Exceeding the limit rejects and rolls
back the new bulk operation, retaining the previous undo.

## Legacy cases

On first access, old `projects.json` metadata, `flags.db`/`flags.json` annotations
and JSON sidecar settings migrate transactionally. Migration markers prevent
later legacy edits from overriding canonical values. Original legacy files are
retained unchanged as backups; future saves write SQLite only. Corrupt or newer
view settings retain their original bytes and cannot be overwritten by an older
frontend. Corrupt annotation entries cause a clear migration failure, with no
partial annotation import.

A legacy Sled migration obtains the original database's exclusive lock before
copying it, reads a private temporary snapshot, and retains the lock until the
SQLite commit. It never opens or recovers the original Sled database. An active
previous application therefore blocks migration with an explicit message.
Derived IOC Sled caches are retained but can be rebuilt into SQLite.

An unreadable legacy registry is copied to `projects.json.corrupt.<uuid>` without
removing the original. Startup reports the error unless all cases are already
fully discoverable from canonical metadata. Corrupt SQLite databases and newer
schemas report errors and are never replaced with empty cases. Settings-only
incomplete case directories are preserved and diagnosed, rather than invented
as cases or deleted.

A staged import writes Parquet, metadata, annotations and settings into
`.staging/<uuid>`. Only the final directory rename publishes the complete case;
there is no later registry write. Cancellation before that rename removes the
staged case and preserves the source evidence. Backups or manual
copies of an open case must include SQLite's WAL files, or be taken after the
application closes the case's active operations. Legacy files alone do not
contain subsequent SQLite edits.

## Deleting cases

Deletion first renames the complete case into the workspace's private
`.trash/<case-uuid>-<operation-uuid>/` directory. This atomic move removes the
case from discovery. A failed move leaves the visible case, database and data
unchanged. No preliminary database tombstone hides a case before that move.

After the move, Trivium removes the owned trash directory. A cleanup failure
leaves the case logically deleted and logs the exact remaining directory for
manual cleanup. It does not restore partially removed files as a usable case.
There is no automatic background trash cleanup. The original imported source
file is never moved or deleted by this operation.
