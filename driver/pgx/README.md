# PGX PostgreSQL Driver

* Runs migrations in transcations.
  That means that if a migration failes, it will be safely rolled back.
* Tries to return helpful error messages.
* Stores migration version details in table ``schema_migrations``.
  This table will be auto-generated.
