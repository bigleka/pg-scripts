-- O objetivo desse conjunto de scripts é criar uma alternativa para armazenar Blobs no banco de dados sem ter que criar colunas varchar(max) text etc.
-- ao invés da inserção ocorrer diretamente na tabela, a operação acontece por uma função que vai receber o objeto Blob, ela vai separar a operação em diversos
-- pedaços limitados pelo tamanho da coluna na tabela de registro e vai inserir esses pedaços da tabela segundo uma ordem sequencial
-- para reaver o registro remontado de uma forma utilizável, o select deve acontecer contra uma função que vai remontar o resultado para uma forma entendível.

-- Esse modelo se mostra vantajoso nos cenários onde, sem a necessidade de uma coluna Blob, podemos usar compressão de tabelas melhores, manutenção online de índices, etc.
-- Esse modelo acaba acarretando em um pensamento diferente quando trata-se de atualizar o registro Blob, uma vez que a limitante é o tamanho da coluna, o interessante fica
-- em marcar o registro como inativado e adicionar outro como ativo, ou apagar o anterior e inserir o novo registro

create extension pgcrypto; -- apenas para usar o UUID mas se quiser trocar por qualquer outro identificador é com você

-- Tabela para demonstrar
CREATE TABLE dados (
    id SERIAL PRIMARY KEY,
    sequencial INT NOT NULL,
    parte VARCHAR(1000) NOT NULL,
    identificador UUID DEFAULT gen_random_uuid() -- certifique-se de que a extensão 'pgcrypto' esteja habilitada ou alguma coisa que te agrade
);

-- Função que vai gerar a separação do blob
CREATE OR REPLACE FUNCTION inserir_dados(dado TEXT)
RETURNS VOID AS $$
DECLARE
    identificador UUID := gen_random_uuid();
    tamanho_parte INT := 1000;
    parte TEXT;
    sequencial INT := 1;
BEGIN
    WHILE LENGTH(dado) > 0 LOOP
        parte := LEFT(dado, tamanho_parte);
        dado := SUBSTRING(dado FROM tamanho_parte + 1);

        INSERT INTO dados (sequencial, parte, identificador)
        VALUES (sequencial, parte, identificador);

        sequencial := sequencial + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Exemplo da operação de inserir usando split do blob
SELECT inserir_dados('<seu_xml_ou_json_aqui>');


-- Função para remontar os dados do Blob
CREATE OR REPLACE FUNCTION reconstruir_dados(identificador UUID)
RETURNS TEXT AS $$
DECLARE
    resultado TEXT;
BEGIN
    SELECT STRING_AGG(parte, '' ORDER BY sequencial) INTO resultado
    FROM dados
    WHERE identificador = identificador;

    RETURN resultado;
END;
$$ LANGUAGE plpgsql;
