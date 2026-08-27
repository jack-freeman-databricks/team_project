-- Writable vs synced table analysis
-- Shows which application tables are writable and their CDC configuration

SELECT
    t.tablename as table_name,
    has_table_privilege(current_user, 'plant.' || t.tablename, 'INSERT') as can_insert,
    has_table_privilege(current_user, 'plant.' || t.tablename, 'UPDATE') as can_update,
    has_table_privilege(current_user, 'plant.' || t.tablename, 'DELETE') as can_delete,
    CASE rel.relreplident
        WHEN 'd' THEN 'DEFAULT'
        WHEN 'n' THEN 'NOTHING'
        WHEN 'f' THEN 'FULL'
        WHEN 'i' THEN 'INDEX'
        ELSE 'UNKNOWN'
    END as replica_identity,
    (SELECT COUNT(*) FROM pg_constraint c
     WHERE c.conrelid = (SELECT oid FROM pg_class WHERE relname = t.tablename AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'plant'))
     AND c.contype = 'f') as foreign_key_count
FROM pg_tables t
JOIN pg_class rel ON rel.relname = t.tablename
JOIN pg_namespace n ON n.oid = rel.relnamespace
WHERE t.schemaname = 'plant'
    AND t.tablename NOT LIKE '%dim_tag%'
ORDER BY t.tablename;
