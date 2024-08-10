CREATE OR REPLACE FUNCTION usp_sugerir_nova_ordem_chave_primaria (schema_name text, nome_tabela text)
    RETURNS text
    AS $$
DECLARE
    coluna_atual record;
    ordem_sugerida text := '';
    ordem_atual text := '';
    query text;
BEGIN
    -- Prepara a query para obter a ordem atual das colunas da chave primária
    FOR coluna_atual IN
    SELECT
        kcu.column_name
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
    WHERE
        tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_name = nome_tabela
        AND tc.table_schema = schema_name
    ORDER BY
        kcu.ordinal_position LOOP
            ordem_atual := ordem_atual || coluna_atual.column_name || ', ';
        END LOOP;
    -- Remove a última vírgula e espaço da ordem atual
    IF LENGTH(ordem_atual) > 0 THEN
        ordem_atual := substr(ordem_atual, 1, LENGTH(ordem_atual) - 2);
    END IF;
    -- Prepara a query para calcular a densidade das colunas da chave primária
    query := format($f$
        SELECT
            kcu.column_name, (COUNT(DISTINCT % I) * 1.0 / COUNT(*)) AS densidade FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN % I. % I AS t ON TRUE
            WHERE
                tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_name = % L
                AND tc.table_schema = % L GROUP BY kcu.column_name ORDER BY densidade DESC, kcu.column_name $f$, 'column_name', schema_name, nome_tabela, nome_tabela, schema_name);
    -- Executa a consulta e processa os resultados
    FOR coluna_atual IN EXECUTE query LOOP
        ordem_sugerida := ordem_sugerida || coluna_atual.column_name || ', ';
    END LOOP;
    -- Remove a última vírgula e espaço
    IF LENGTH(ordem_sugerida) > 0 THEN
        ordem_sugerida := substr(ordem_sugerida, 1, LENGTH(ordem_sugerida) - 2);
    END IF;
    -- Compara a ordem atual com a sugerida
    IF ordem_atual = ordem_sugerida THEN
        RETURN 'A ordem atual já é a melhor.';
    ELSE
        RETURN ordem_sugerida;
    END IF;
END;
$$
LANGUAGE plpgsql;
