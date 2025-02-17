/*
Para essa função funcionar você precisa que as extensões do FDW e o DBLINK estejam instaladas
a ideia aqui é usar o usar o DBLINK para fazer a conexão com o outro banco de dados mas,
ao invés de passar servidor banco usuário e senha usar o servidor criado pelo FDW para encapsular toda a string de conexão
e com a tabela extrangeira criada pelo FDW como base, para criar a view fazendo o mapeamento das colunas automaticamente.

Ex.
select criar_views_externas('servidor_fdw','schema_onde_estão_as_tabelas_extrangeiras','schema_destino','uma_tabela_qqer' ou NULL (todas as tabelas),0)
*/
CREATE OR REPLACE FUNCTION criar_views_externas(servidor_fdw TEXT, schema_origem TEXT, schema_destino TEXT, tabela_especifica TEXT DEFAULT NULL, executar_script INT DEFAULT 0)
RETURNS VOID AS $$
DECLARE
    tabela RECORD;
    sql TEXT;
    servidor RECORD;
BEGIN
    -- Obtém as informações do servidor FDW
    SELECT srvname INTO servidor
    FROM pg_foreign_server
    WHERE srvname = servidor_fdw; 

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Servidor FDW não encontrado.';
    END IF;

    FOR tabela IN
        SELECT foreign_table_name
        FROM information_schema.foreign_tables
        WHERE foreign_table_schema = schema_origem
          AND (tabela_especifica IS NULL OR foreign_table_name = tabela_especifica)
          AND foreign_table_name NOT LIKE 'google%'
    LOOP
        -- Constrói a consulta para criar a view no esquema de destino
        sql := 'CREATE OR REPLACE VIEW ' || quote_ident(schema_destino) || '.' || quote_ident(tabela.foreign_table_name) || ' AS ' ||
               'SELECT * FROM dblink(' ||
               '''' || servidor.srvname || ''', ' ||  -- Mantendo a variável servidor
               '''SELECT * FROM ' || quote_ident(tabela.foreign_table_name) || ''') AS (' ||
               (SELECT STRING_AGG(column_name || ' ' ||
                                 CASE
                                     WHEN data_type = 'character varying' THEN 'VARCHAR(' || character_maximum_length || ')'
                                     WHEN data_type = 'numeric' THEN 'NUMERIC(' || numeric_precision || ', ' || numeric_scale || ')'
                                     WHEN data_type IN ('timestamp', 'timestamp with time zone') THEN 'TIMESTAMP'
                                     WHEN data_type = 'interval' THEN 'INTERVAL'
                                     WHEN data_type IN ('json', 'jsonb') THEN 'JSONB'
                                     WHEN data_type = 'boolean' THEN 'BOOLEAN'
                                     WHEN data_type = 'bytea' THEN 'BYTEA'
                                     WHEN data_type LIKE 'ARRAY%' THEN 'ARRAY'  -- Tratar arrays de forma específica se necessário
                                     ELSE data_type
                                 END, ', ')
                FROM information_schema.columns
                WHERE table_name = tabela.foreign_table_name AND table_schema = schema_origem) || ');';

        -- Exibe o SQL gerado
        RAISE NOTICE '-- %', sql;

        -- Executa a consulta ou apenas exibe
        IF executar_script = 0 THEN
            EXECUTE sql;
        ELSE
            RAISE NOTICE '%', sql;  -- Exibe o SQL gerado se executar_script for diferente de 0
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
