/*
Principal part of a big script.
To start the partition of a existing table we will inform:
- original table
- column that we´ll use to partition
- from date !!! inform the date like 2020-06-01 ALWAYS put the date start from day 1
- to date - I recomend put the date to the last day of the year 2024-12-31

this script will call other 3 functions:
- create partitioned table
- create partitions for the partitioned table
- copy the data

and then rename the original table and PK _old 
and rename the new partitioned table to the original table
*/
CREATE OR REPLACE FUNCTION partition_existing_table(original_table TEXT, partition_column TEXT, from_date DATE, to_date DATE, execute_commands INTEGER)
RETURNS VOID AS $$
DECLARE
    new_table TEXT := original_table || '_new';
BEGIN
    -- Criar a nova tabela particionada
    PERFORM create_partitioned_table(original_table, partition_column);

    -- Gerar e executar os scripts de particionamento
    PERFORM generate_and_execute_partition_scripts(new_table, partition_column, from_date, to_date, execute_commands);

    -- Copiar dados da tabela original para a nova tabela particionada
    PERFORM copy_data_to_new_table(original_table, new_table);

    -- Renomear as tabelas
    EXECUTE 'ALTER TABLE ' || original_table || ' RENAME CONSTRAINT pk_' || original_table || ' to pk_'|| original_table ||'_old;';
    EXECUTE 'ALTER TABLE ' || original_table || ' RENAME TO ' || original_table || '_old;';
    EXECUTE 'ALTER TABLE ' || new_table || ' RENAME constraint pk_' || new_table || ' to pk_'|| original_table ||';';
    EXECUTE 'ALTER TABLE ' || new_table || ' RENAME TO ' || original_table || ';';
END;
$$ LANGUAGE plpgsql;

/*
-- Exemplo de uso da função principal
SELECT partition_existing_table('tabela_teste', 'coluna_de_data', '2021-06-01', '2024-12-31', 1);
*/
