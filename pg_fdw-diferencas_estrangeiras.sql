/*
Script ainda em desenvolvimento !

Motivo: Como o PG não possui interconexão de banco, hoje usamos o FDW para criar objetos estrangeiros de um banco A no banco B
isso cria de forma transparente uma facilidade de acesso de uma query entre os bancos.
O problema fica na questão que quando um novo objeto é criado ou alterado no banco principal o objeto estrangeiro não reflete essa alteração
já que não existe nenhum gatilho que faça isso acontecer.
Criar um job para ser executado recorrente que apague todos os objetos estrangeiros e importe novamente tudo é uma opção mas que tem um alto custo
operacional, já que se o objeto estiver sendo acessado ele vai gerar lock e esperar mas como ele já apagou outros objetos se alguma outra query tentar
acessar algum objeto estrangeiro ele não existirá

Objetivo: Esse script vai criar uma conexão dblink a partir do banco onde estão os objetos estrangeiros e o banco de origem, mapear os objetos que estão
em um schema diferente do public e vai listar essa diferença.

em um segundo momento, esse script vai apagar a tabela estrangeira e vai recriar apenas aquela entrada daquele objeto.
*/

-- Executar caso ainda não tenha adicionado o dblink
create extension dblink;

-- Ajuste aqui o schema local dos objetos estrangeiros
WITH config AS (
  SELECT 'schema_usado_para_hospedar_os_objetos_estrangeiros'::text AS local_schema, -- alterar aqui o schema onde estão os objetos estrangeiros
         'public'::text     AS remote_schema
),

foreign_tables AS (
  SELECT
    ns.nspname  AS table_schema,
    cls.relname AS table_name
  FROM pg_foreign_table ft
  JOIN pg_class cls    ON cls.oid = ft.ftrelid
  JOIN pg_namespace ns ON ns.oid = cls.relnamespace
),

local_columns AS (
  SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type,
    c.is_nullable
  FROM information_schema.columns c
  JOIN config conf ON c.table_schema = conf.local_schema
  JOIN foreign_tables f
    ON c.table_schema = f.table_schema
   AND c.table_name   = f.table_name
),

remote_columns_raw AS (
  SELECT *
  FROM dblink(
    'servidor_fdw', --alterar aqui o serviidor do FDW
    $$
    SELECT table_schema, table_name, column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public'
    $$
  ) AS t(
    table_schema text,
    table_name   text,
    column_name  text,
    data_type    text,
    is_nullable  text
  )
),

remote_columns AS (
  SELECT *
  FROM remote_columns_raw rc
  JOIN config conf ON rc.table_schema = conf.remote_schema
),

differences AS (
  SELECT
    COALESCE(lc.table_schema, rc.table_schema) AS table_schema,
    COALESCE(lc.table_name, rc.table_name)     AS table_name,
    COALESCE(lc.column_name, rc.column_name)   AS column_name,
    lc.data_type        AS local_data_type,
    rc.data_type        AS remote_data_type,
    lc.is_nullable      AS local_is_nullable,
    rc.is_nullable      AS remote_is_nullable
  FROM local_columns lc
  FULL OUTER JOIN remote_columns rc
    ON lc.table_name   = rc.table_name
   AND lc.column_name  = rc.column_name
),

consolidado AS (
  SELECT
    CASE
      WHEN local_data_type IS NULL THEN 'MISSING_ON_LOCAL'
      WHEN remote_data_type IS NULL THEN 'MISSING_ON_REMOTE'
      WHEN local_data_type <> remote_data_type
        OR local_is_nullable <> remote_is_nullable THEN 'MISMATCH'
    END AS change_type,
    *
  FROM differences
  WHERE
    local_data_type IS NULL
    OR remote_data_type IS NULL
    OR local_data_type <> remote_data_type
    OR local_is_nullable <> remote_is_nullable
)

SELECT *
FROM consolidado
where table_schema is not null
and table_name not like 'pg%' -- não traz tabelas do pg
and table_name not like 'google%' -- não trazer tabelas da GCP
and table_name not like 'hypopg%' -- não trazer tabelas da GCP sistema de monitoração da GCP
and table_name !~ '\d+$' -- não trazer tabelas que acabam com números
and table_name not in ('blocking_procs') -- não trazer alguma tabela especial
ORDER BY table_schema, table_name, column_name;
