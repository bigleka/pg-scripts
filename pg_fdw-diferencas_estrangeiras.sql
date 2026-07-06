/*

Esta função verifica se houve alteração nos objetos estrangeiros (foreign tables)
e tenta dropar e reimportar o objeto que foi alterado.

A ideia é ser simples e sem intervenção humana: roda de tempos em tempos nos dois
bancos. Cada banco tem um foreign server apontando para o outro lado.

Mudancas em relacao a v2:
  - Log unificado em dba.mc1_log (tabela GENERICA reutilizavel por outras rotinas
    de manutencao do schema dba), no modelo HIBRIDO:
      * colunas fixas comuns a qualquer rotina: source, status, duration_ms,
        executed_at, database_name -> consultaveis e indexadas diretamente;
      * payload jsonb -> tudo que e especifico da rotina (table_name, action,
        local_schema, fdw_server, remote_schema, affected_columns, dependents,
        details), consultavel via indice GIN.
    Uma linha por objeto processado (granularidade mantida da v2).
  - dba.mc1_sync_log foi aposentada; migracao opcional no rodape do script.

Demais caracteristicas herdadas da v2:
  - Introspeccao remota via dblink reaproveitando a conexao do FDW (server name),
    sem credencial em texto claro.
  - Comparacao de tipo: udt_name, character_maximum_length, numeric_precision,
    numeric_scale e is_nullable com IS DISTINCT FROM. ordinal_position NAO entra
    na comparacao (mapeamento postgres_fdw e por NOME de coluna).
  - Autodeteccao de schema/servidor abortando em caso de ambiguidade.
  - Acoes: DROP_RECREATE, NEW_TABLE, DROP_ORPHAN.
  - p_dry_run: simula sem aplicar DDL e sem gravar no log.
  - Registro de dependentes (views/regras) antes do CASCADE.
  - Advisory lock de transacao por par (schema, servidor).
  - Tratamento de erro agregado e retorno tabular.

*/

-- =====================================================================
-- BOOTSTRAP: schema, extensão, tabela de log GENERICA e índices
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS dba;

-- Tabela de log genérica (híbrida): colunas fixas comuns + payload jsonb.
-- Reutilizavel por qualquer rotina de manutencao futura do schema dba.
CREATE TABLE IF NOT EXISTS dba.mc1_log (
    id            BIGSERIAL   PRIMARY KEY,
    executed_at   timestamptz NOT NULL DEFAULT now(),
    database_name text        NOT NULL DEFAULT current_database(),
    source        text        NOT NULL,               -- nome da rotina: 'mc1_sync_foreign_tables'
    status        text        NOT NULL,               -- OK, ERROR (dry-run nao e persistido)
    duration_ms   numeric,                            -- comum e muito consultado -> coluna fixa
    payload       jsonb       NOT NULL DEFAULT '{}'::jsonb
);

-- Índices de apoio
CREATE INDEX IF NOT EXISTS idx_mc1_log_executed_at
    ON dba.mc1_log (executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_mc1_log_source
    ON dba.mc1_log (source, executed_at DESC);

-- GIN no payload: permite filtrar por qualquer chave do JSON sem prever tudo.
-- Ex.: WHERE payload @> '{"action":"DROP_RECREATE"}'
CREATE INDEX IF NOT EXISTS idx_mc1_log_payload
    ON dba.mc1_log USING gin (payload);

-- Limpa assinaturas antigas para evitar funções órfãs
DROP FUNCTION IF EXISTS dba.mc1_sync_foreign_tables(text, text[], text[]);
DROP FUNCTION IF EXISTS dba.mc1_sync_foreign_tables(text, text[]);
DROP FUNCTION IF EXISTS dba.mc1_sync_foreign_tables(text, text, text[]);
DROP FUNCTION IF EXISTS dba.mc1_sync_foreign_tables(text, text, text, text[], boolean);
DROP FUNCTION IF EXISTS dba.mc1_sync_foreign_tables(text, text, text, text[]);

-- =====================================================================
-- FUNÇÃO PRINCIPAL (v3 - log genérico)
-- =====================================================================

CREATE OR REPLACE FUNCTION dba.mc1_sync_foreign_tables(
    p_remote_schema  text    DEFAULT 'public',
    p_local_schema   text    DEFAULT NULL,          -- NULL = autodetecta a partir das foreign tables
    p_fdw_server     text    DEFAULT NULL,          -- NULL = autodetecta a partir das foreign tables
    p_exclude_tables text[]  DEFAULT ARRAY['blocking_procs'],
    p_dry_run        boolean DEFAULT FALSE          -- TRUE = simula as alterações sem aplicar DDL
)
RETURNS TABLE (
    table_name       text,
    action           text,
    status           text,
    affected_columns text,
    error            text
) AS $func$
DECLARE
    r_diff           record;
    ddl_command      text;
    v_local_schema   text := p_local_schema;
    v_fdw_server     text := p_fdw_server;
    v_start_time     timestamp := clock_timestamp();
    v_loop_start     timestamp;
    v_duration_ms    numeric;
    v_ok_count       int := 0;
    v_err_count      int := 0;
    v_lock_obtained  boolean;
    v_lock_key       int;
    v_schema_count   int;
    v_server_count   int;
    v_dependents     text;
    v_details        text;

    -- Nome da rotina, gravado na coluna fixa `source` do log genérico.
    c_source         constant text := 'mc1_sync_foreign_tables';

    -- Query de introspecção no lado remoto: montada dinamicamente no BEGIN,
    -- pois precisa respeitar o parâmetro p_remote_schema.
    dblink_query text;
BEGIN
    RAISE LOG 'mc1_sync_foreign_tables: iniciando sincronizacao de tabelas estrangeiras...';

    -- Garante a extensão no banco (deve ser executado como superuser/owner)
    CREATE EXTENSION IF NOT EXISTS dblink;

    -- -----------------------------------------------------------------
    -- Autodetecção de schema local e servidor FDW, somente se não foram
    -- informados. Aborta com erro claro se houver ambiguidade.
    -- -----------------------------------------------------------------
    IF v_local_schema IS NULL THEN
        SELECT count(DISTINCT ft.foreign_table_schema)
          INTO v_schema_count
          FROM information_schema.foreign_tables ft
         WHERE ft.foreign_table_schema NOT LIKE 'pg_%';

        IF v_schema_count > 1 THEN
            RAISE EXCEPTION 'Múltiplos schemas com foreign tables encontrados (%). Informe p_local_schema explicitamente.', v_schema_count;
        END IF;

        SELECT ft.foreign_table_schema
          INTO v_local_schema
          FROM information_schema.foreign_tables ft
         WHERE ft.foreign_table_schema NOT LIKE 'pg_%'
         LIMIT 1;
    END IF;

    IF v_fdw_server IS NULL THEN
        SELECT count(DISTINCT ft.foreign_server_name)
          INTO v_server_count
          FROM information_schema.foreign_tables ft
         WHERE ft.foreign_table_schema = v_local_schema;

        IF v_server_count > 1 THEN
            RAISE EXCEPTION 'Múltiplos servidores FDW encontrados no schema % (%). Informe p_fdw_server explicitamente.', v_local_schema, v_server_count;
        END IF;

        SELECT ft.foreign_server_name
          INTO v_fdw_server
          FROM information_schema.foreign_tables ft
         WHERE ft.foreign_table_schema = v_local_schema
         LIMIT 1;
    END IF;

    IF v_local_schema IS NULL OR v_fdw_server IS NULL THEN
        RAISE LOG 'mc1_sync_foreign_tables: nenhuma tabela estrangeira encontrada e parâmetros omitidos. Nada a fazer.';
        RETURN;
    END IF;

    -- Lock de transação: chave derivada do par (schema, servidor) alvo.
    v_lock_key := hashtext(v_local_schema || '|' || v_fdw_server);

    SELECT pg_try_advisory_xact_lock(v_lock_key) INTO v_lock_obtained;
    IF NOT v_lock_obtained THEN
        RAISE NOTICE 'Já existe uma sincronização em execução para %.% via %. Abortando.', v_local_schema, p_remote_schema, v_fdw_server;
        RETURN;
    END IF;

    RAISE LOG 'mc1_sync_foreign_tables: Schema Local=%, Schema Remoto=%, Servidor FDW=%, Dry Run=%',
              v_local_schema, p_remote_schema, v_fdw_server, p_dry_run;

    -- Monta a query remota já parametrizada com p_remote_schema.
    dblink_query := format(
        $dblink$
            SELECT table_schema,
                   table_name,
                   column_name,
                   ordinal_position,
                   udt_name,
                   character_maximum_length,
                   numeric_precision,
                   numeric_scale,
                   is_nullable
            FROM information_schema.columns
            WHERE table_schema = %L
        $dblink$,
        p_remote_schema
    );

    -- Itera sobre tabelas modificadas, órfãs ou novas detectadas no remoto
    FOR r_diff IN
        WITH foreign_tables AS (
            SELECT ft.foreign_table_schema AS table_schema,
                   ft.foreign_table_name   AS tgt_table_name
              FROM information_schema.foreign_tables ft
             WHERE ft.foreign_table_schema = v_local_schema
               AND ft.foreign_server_name  = v_fdw_server
        ),
        local_columns AS (
            SELECT c.table_schema,
                   c.table_name AS tgt_table_name,
                   c.column_name,
                   c.ordinal_position,
                   c.udt_name,
                   c.character_maximum_length,
                   c.numeric_precision,
                   c.numeric_scale,
                   c.is_nullable
              FROM information_schema.columns c
              JOIN foreign_tables ft
                ON c.table_schema = ft.table_schema
               AND c.table_name   = ft.tgt_table_name
        ),
        remote_columns AS (
            SELECT rc.table_name AS tgt_table_name,
                   rc.column_name,
                   rc.ordinal_position,
                   rc.udt_name,
                   rc.character_maximum_length,
                   rc.numeric_precision,
                   rc.numeric_scale,
                   rc.is_nullable
              FROM dblink(v_fdw_server, dblink_query) AS rc(
                       table_schema             text,
                       table_name               text,
                       column_name              text,
                       ordinal_position         int,
                       udt_name                 text,
                       character_maximum_length int,
                       numeric_precision        int,
                       numeric_scale            int,
                       is_nullable              text
                   )
        ),
        -- Identifica tabelas órfãs (existem local, mas sumiram no remoto)
        orphan_tables AS (
            SELECT ft.tgt_table_name
              FROM foreign_tables ft
              LEFT JOIN (SELECT DISTINCT tgt_table_name FROM remote_columns) rc ON ft.tgt_table_name = rc.tgt_table_name
             WHERE rc.tgt_table_name IS NULL
        ),
        -- Identifica tabelas novas (existem no remoto, mas não local)
        new_tables AS (
            SELECT DISTINCT rc.tgt_table_name
              FROM remote_columns rc
              LEFT JOIN foreign_tables ft ON rc.tgt_table_name = ft.tgt_table_name
             WHERE ft.tgt_table_name IS NULL
        ),
        -- Divergências estruturais de colunas existentes (calcula colunas afetadas)
        column_mismatches AS (
            SELECT DISTINCT
                   COALESCE(lc.tgt_table_name, rc.tgt_table_name) AS tgt_table_name,
                   string_agg(COALESCE(lc.column_name, rc.column_name), ', ') AS affected_cols
              FROM local_columns lc
              FULL OUTER JOIN remote_columns rc
                ON lc.tgt_table_name  = rc.tgt_table_name
               AND lc.column_name = rc.column_name
             WHERE lc.column_name IS NULL
                OR rc.column_name IS NULL
                OR lc.udt_name                 IS DISTINCT FROM rc.udt_name
                OR lc.character_maximum_length IS DISTINCT FROM rc.character_maximum_length
                OR lc.numeric_precision        IS DISTINCT FROM rc.numeric_precision
                OR lc.numeric_scale            IS DISTINCT FROM rc.numeric_scale
                OR lc.is_nullable              IS DISTINCT FROM rc.is_nullable
                -- NOTA: ordinal_position foi removido do critério de comparação.
                -- Para foreign tables via postgres_fdw, o mapeamento de dados é
                -- por NOME de coluna, não por posição.
             GROUP BY COALESCE(lc.tgt_table_name, rc.tgt_table_name)
        ),
        -- Determina a ação final por tabela (prioridade: DROP_ORPHAN > NEW_TABLE > DROP_RECREATE)
        base_actions AS (
            SELECT tgt_table_name,
                   CASE
                       WHEN bool_or(raw_action = 'DROP_ORPHAN') THEN 'DROP_ORPHAN'
                       WHEN bool_or(raw_action = 'NEW_TABLE')   THEN 'NEW_TABLE'
                       ELSE 'DROP_RECREATE'
                   END AS base_action
            FROM (
                SELECT cm.tgt_table_name, 'DROP_RECREATE'::text AS raw_action FROM column_mismatches cm
                UNION ALL
                SELECT nt.tgt_table_name, 'NEW_TABLE'::text     AS raw_action FROM new_tables nt
                UNION ALL
                SELECT ot.tgt_table_name, 'DROP_ORPHAN'::text   AS raw_action FROM orphan_tables ot
            ) x
            GROUP BY tgt_table_name
        ),
        -- affected_cols só faz sentido para DROP_RECREATE (divergência de colunas).
        all_targets AS (
            SELECT ba.tgt_table_name,
                   ba.base_action,
                   CASE WHEN ba.base_action = 'DROP_RECREATE' THEN cm.affected_cols ELSE NULL END AS affected_cols
              FROM base_actions ba
              LEFT JOIN column_mismatches cm ON cm.tgt_table_name = ba.tgt_table_name
        )
        SELECT DISTINCT
               t.tgt_table_name,
               t.base_action,
               t.affected_cols
          FROM all_targets t
         WHERE t.tgt_table_name IS NOT NULL
           -- Filtro 1: garante estritamente o padrão corporativo de prefixos
           AND t.tgt_table_name ~* '^(etl_|mc1_)'
           -- Filtro 2: ignora partições (_p01), backups (_old, _backup) e temporárias locais
           AND t.tgt_table_name !~* '(^pg_|^google|^hypopg|^tmp|^tt|_p\d{1,2}$|_old$|_backup$|_tmp\d*$)'
           -- Filtro 3: ignora tabelas enviadas por parâmetro manual
           AND t.tgt_table_name <> ALL (p_exclude_tables)
         ORDER BY t.tgt_table_name
    LOOP
        v_loop_start := clock_timestamp();
        v_dependents := NULL;

        -- Antes de qualquer DROP, registra objetos dependentes (views, regras
        -- etc.) que seriam removidos em cascata.
        IF r_diff.base_action IN ('DROP_ORPHAN', 'DROP_RECREATE') THEN
            SELECT string_agg(DISTINCT dep_obj.relname, ', ')
              INTO v_dependents
              FROM pg_depend d
              JOIN pg_rewrite rw ON rw.oid = d.objid AND d.classid = 'pg_rewrite'::regclass
              JOIN pg_class dep_obj ON dep_obj.oid = rw.ev_class
              JOIN pg_class src ON src.oid = d.refobjid
              JOIN pg_namespace ns ON ns.oid = src.relnamespace
             WHERE ns.nspname = v_local_schema
               AND src.relname = r_diff.tgt_table_name
               AND dep_obj.relname <> r_diff.tgt_table_name;

            IF v_dependents IS NOT NULL THEN
                RAISE WARNING 'Tabela %.% possui objetos dependentes que serão removidos em cascata: %',
                              v_local_schema, r_diff.tgt_table_name, v_dependents;
            END IF;
        END IF;

        -- Texto-base do "details".
        v_details := CASE r_diff.base_action
                         WHEN 'DROP_RECREATE' THEN r_diff.affected_cols
                         WHEN 'NEW_TABLE'      THEN 'Nova tabela encontrada no remoto'
                         WHEN 'DROP_ORPHAN'    THEN 'Tabela deletada do servidor remoto'
                     END;

        IF v_dependents IS NOT NULL THEN
            v_details := v_details || ' | Dependentes: ' || v_dependents;
        END IF;

        -- Determina o comando DDL apropriado
        IF r_diff.base_action = 'DROP_ORPHAN' THEN
            ddl_command := format('DROP FOREIGN TABLE IF EXISTS %I.%I CASCADE;', v_local_schema, r_diff.tgt_table_name);
        ELSE
            ddl_command := format(
                'DROP FOREIGN TABLE IF EXISTS %I.%I CASCADE; ' ||
                'IMPORT FOREIGN SCHEMA %I LIMIT TO (%I) FROM SERVER %I INTO %I;',
                v_local_schema, r_diff.tgt_table_name, p_remote_schema, r_diff.tgt_table_name, v_fdw_server, v_local_schema
            );
        END IF;

        BEGIN
            IF p_dry_run THEN
                RAISE NOTICE '[DRY_RUN] % para %.% (Detalhes: %)',
                             r_diff.base_action, v_local_schema, r_diff.tgt_table_name, v_details;

                table_name       := r_diff.tgt_table_name;
                action           := r_diff.base_action;
                status           := 'DRY_RUN';
                affected_columns := r_diff.affected_cols;
                error            := NULL;

                -- Dry run não grava no log: é apenas simulação.
                RETURN NEXT;
            ELSE
                RAISE LOG '[%] Executando % em: %.%', clock_timestamp()::time, r_diff.base_action, v_local_schema, r_diff.tgt_table_name;
                EXECUTE ddl_command;

                v_ok_count    := v_ok_count + 1;
                v_duration_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_loop_start)) * 1000;

                table_name       := r_diff.tgt_table_name;
                action           := r_diff.base_action;
                status           := 'OK';
                affected_columns := r_diff.affected_cols;
                error            := NULL;

                -- Log genérico: colunas fixas + payload jsonb com o específico.
                INSERT INTO dba.mc1_log (source, status, duration_ms, payload)
                VALUES (
                    c_source,
                    'OK',
                    v_duration_ms,
                    jsonb_build_object(
                        'local_schema',     v_local_schema,
                        'fdw_server',       v_fdw_server,
                        'remote_schema',    p_remote_schema,
                        'table_name',       r_diff.tgt_table_name,
                        'action',           r_diff.base_action,
                        'affected_columns', r_diff.affected_cols,
                        'dependents',       v_dependents,
                        'details',          v_details
                    )
                );

                RETURN NEXT;
            END IF;

        EXCEPTION WHEN OTHERS THEN
            v_err_count   := v_err_count + 1;
            v_duration_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_loop_start)) * 1000;

            error := format('DDL: %s | ERRO: %s', ddl_command, SQLERRM);

            RAISE WARNING 'Erro na ação % para %.%: %', r_diff.base_action, v_local_schema, r_diff.tgt_table_name, SQLERRM;

            table_name       := r_diff.tgt_table_name;
            action           := r_diff.base_action;
            status           := 'ERROR';
            affected_columns := r_diff.affected_cols;

            INSERT INTO dba.mc1_log (source, status, duration_ms, payload)
            VALUES (
                c_source,
                'ERROR',
                v_duration_ms,
                jsonb_build_object(
                    'local_schema',     v_local_schema,
                    'fdw_server',       v_fdw_server,
                    'remote_schema',    p_remote_schema,
                    'table_name',       r_diff.tgt_table_name,
                    'action',           r_diff.base_action,
                    'affected_columns', r_diff.affected_cols,
                    'dependents',       v_dependents,
                    'details',          v_details,
                    'error',            error
                )
            );

            RETURN NEXT;
        END;
    END LOOP;

    RAISE LOG 'mc1_sync_foreign_tables: concluido. OK: %, Erros: %. Tempo total: %',
              v_ok_count, v_err_count, clock_timestamp() - v_start_time;
END;
$func$ LANGUAGE plpgsql;


/*
=====================================================================
EXEMPLOS DE EXECUÇÃO
=====================================================================
*/

-- Simulação (dry-run): mostra o que seria feito, sem aplicar DDL e sem gravar log.
SELECT * FROM dba.mc1_sync_foreign_tables(p_dry_run => TRUE);

-- Autodeteccao total (um unico FDW por banco, como no cenario atual):
SELECT * FROM dba.mc1_sync_foreign_tables();

-- Forma explicita:
-- SELECT * FROM dba.mc1_sync_foreign_tables(
--     p_remote_schema => 'public',
--     p_local_schema  => 'etl',
--     p_fdw_server    => 'bo2etl'
-- );


/*
=====================================================================
CONSULTAS DE AUDITORIA / DIAGNÓSTICO no log genérico
=====================================================================
*/

-- Últimas execuções desta rotina (colunas fixas + campos do payload):
 SELECT executed_at,
        status,
        duration_ms,
        payload->>'table_name' AS table_name,
        payload->>'action'     AS action,
        payload->>'details'    AS details
   FROM dba.mc1_log
  WHERE source = 'mc1_sync_foreign_tables'
  ORDER BY executed_at DESC;
    

-- Quantos objetos por ação numa janela (quantifica falso-positivo de DROP_RECREATE):
SELECT payload->>'action' AS action, count(*), round(avg(duration_ms)) AS avg_ms, round(sum(duration_ms)) AS total_ms
  FROM dba.mc1_log
  WHERE source = 'mc1_sync_foreign_tables'
   AND executed_at > now() - interval '1 day'
  GROUP BY payload->>'action'
  ORDER BY total_ms DESC;

-- Só os erros (usa o índice GIN):
SELECT executed_at, payload->>'table_name' AS table_name, payload->>'error' AS error
  FROM dba.mc1_log
  WHERE source = 'mc1_sync_foreign_tables'
    AND payload @> '{"action":"DROP_RECREATE"}'
    AND status = 'ERROR'
  ORDER BY executed_at DESC;
