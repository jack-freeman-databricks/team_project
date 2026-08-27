-- Operational schema introspection for plant schema
-- Shows all tables, constraints, and indexes that define the domain model

SELECT
    t.tablename as table_name,
    COALESCE(
        (SELECT COUNT(*)
         FROM pg_constraint c
         WHERE c.conrelid = (SELECT oid FROM pg_class WHERE relname = t.tablename AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'plant'))
         AND c.contype = 'p'),
        0
    ) as pk_count,
    COALESCE(
        (SELECT COUNT(*)
         FROM pg_constraint c
         WHERE c.conrelid = (SELECT oid FROM pg_class WHERE relname = t.tablename AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'plant'))
         AND c.contype = 'f'),
        0
    ) as fk_count,
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'plant' AND table_name = t.tablename) as column_count,
    (SELECT COUNT(*) FROM pg_stat_user_tables WHERE schemaname = 'plant' AND relname = t.tablename) as row_count
FROM pg_tables t
WHERE t.schemaname = 'plant'
ORDER BY t.tablename;
