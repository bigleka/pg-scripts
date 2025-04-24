/*
This function is part of the partition_existing_table function.
This part is responsible for create a new table based on a existing table and add the column to partition range and primary key
*/

 -- Função para criar uma nova tabela particionada
CREATE OR REPLACE FUNCTION create_partitioned_table(original_table TEXT, partition_column TEXT)
RETURNS VOID AS $$
DECLARE
    table_def TEXT;
    new_table TEXT := original_table || '_new';
    pk_columns TEXT;
BEGIN
    -- Obter a definição da tabela original
    SELECT string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) INTO table_def
    FROM information_schema.columns
    WHERE table_name = original_table;

    IF table_def IS NULL THEN
        RAISE EXCEPTION 'Tabela % não encontrada ou sem colunas', original_table;
    END IF;

    -- Obter a lista de colunas da chave primária
    SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) INTO pk_columns
    FROM information_schema.key_column_usage
    WHERE table_name = original_table
    AND constraint_name = (
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_name = original_table
        AND constraint_type = 'PRIMARY KEY'
    );

    IF pk_columns IS NULL THEN
        RAISE EXCEPTION 'Chave primária não encontrada para a tabela %', original_table;
    END IF;

    -- Adicionar a coluna de particionamento à chave primária
    pk_columns := pk_columns || ', ' || partition_column;

    -- Criar a nova tabela com a coluna de particionamento adicionada à chave primária
    execute 'CREATE TABLE ' || new_table || ' (' || table_def || ', CONSTRAINT pk_'|| new_table ||' PRIMARY KEY (' || pk_columns || ')) PARTITION BY RANGE (' || partition_column || ');';
END;
$$ LANGUAGE plpgsql;
