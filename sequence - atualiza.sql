/*
as vezes precisamos importar dados para tabelas ou recriar tabelas e o sequence fica em um valor diferente do próximo valor da tabela e começamos a ter erros de pk 
esse script reseta o sequence das tabelas para o maior valor da tabela +1
*/

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT
            s.relname AS seq_name,
            ns.nspname AS seq_schema,
            t.relname AS tab_name,
            a.attname AS col_name,
            pg_get_serial_sequence(ns.nspname || '.' || t.relname, a.attname) AS full_seq_name
        FROM
            pg_class s
            JOIN pg_depend d ON d.objid = s.oid
            JOIN pg_class t ON d.refobjid = t.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
            JOIN pg_namespace ns ON ns.oid = s.relnamespace
        WHERE
            s.relkind = 'S' AND ns.nspname = 'public'
    LOOP
        EXECUTE format(
            'SELECT setval(%L, COALESCE((SELECT MAX(%I) FROM %I.%I), 0) + 1, false);',
            r.full_seq_name, r.col_name, r.seq_schema, r.tab_name
        );
        RAISE NOTICE 'Sequence % updated', r.full_seq_name;
    END LOOP;
END $$;
