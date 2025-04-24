/*
This function is part of the partition_existing_table function.
This part is responsible for copy data from the original table to the new table
*/

-- Função para copiar dados da tabela original para a nova tabela particionada
CREATE OR REPLACE FUNCTION copy_data_to_new_table(original_table TEXT, new_table TEXT)
RETURNS VOID AS $$
BEGIN
    EXECUTE 'INSERT INTO ' || new_table || ' SELECT * FROM ' || original_table || ';';
END;
$$ LANGUAGE plpgsql;
