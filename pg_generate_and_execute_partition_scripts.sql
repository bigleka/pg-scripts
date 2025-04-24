/*
This function is part of the partition_existing_table function.
This part is responsible for open the partition to the new table

This script cloud the executed apart to open partition to existing partitioned tables
*/
-- Função para gerar e executar os scripts de particionamento
CREATE OR REPLACE FUNCTION generate_and_execute_partition_scripts(new_table TEXT, partition_column TEXT, from_date DATE, to_date DATE, execute_commands INTEGER)
RETURNS TEXT AS $$
DECLARE
    script TEXT := '';
    curr_date DATE := from_date;
    pk_columns TEXT;
BEGIN
    -- Obter a lista de colunas da chave primária
    SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) INTO pk_columns
    FROM information_schema.key_column_usage
    WHERE table_name = new_table
    AND constraint_name = (
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_name = new_table
        AND constraint_type = 'PRIMARY KEY'
    );

    -- Remover a coluna de particionamento da lista de colunas da chave primária
    pk_columns := REPLACE(pk_columns, partition_column || ', ', '');
    pk_columns := REPLACE(pk_columns, ', ' || partition_column, '');

    -- Loop para cada mês entre from_date e to_date
    WHILE curr_date <= to_date LOOP
        script := script || 'CREATE TABLE ' || REPLACE(new_table, '_new','') || '_y' || to_char(curr_date, 'YYYYmMM') ||
                  ' PARTITION OF ' || new_table || ' FOR VALUES FROM (''' || curr_date || ''') TO (''' || (curr_date + INTERVAL '1 month') || ''');' || E'\n';
        script := script || 'CREATE UNIQUE INDEX unq_' || REPLACE(new_table, '_new','') || '_y' || to_char(curr_date, 'YYYYmMM') ||
                  ' ON ' || REPLACE(new_table, '_new','') || '_y' || to_char(curr_date, 'YYYYmMM') ||
                  ' (' || pk_columns || ');' || E'\n';
        curr_date := curr_date + INTERVAL '1 month';
    END LOOP;

    -- Executar os comandos, se solicitado
    IF execute_commands = 1 THEN
        EXECUTE script;
        RETURN 'Comandos executados com sucesso.';
    ELSE
        RETURN script;
    END IF;
END;
$$ LANGUAGE plpgsql;
