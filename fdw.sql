/*
A extensão do FDW, comparado com o SQL Server, é como se fosse o LinkedServer
como o PG não permite o cross database diretamente, ou você usa o DBLink ou usa o FDW
a grande diferença entre os 2 é que o FDW, teoricamente, se mostra uma forma mais transparente de acesso a objeto que está em outro banco
de uma forma que usuário e senha ficam obscuras para as pessoas e o controle do fluxo de dados entre os bancos pode ser controlado
não estou aqui para defender um ou outro, é só um monte de script para ficar mais fácil de achar em algum lugar, você que pesquise qual vale
mais ou menos para sua necessidade.
*/

--adiciona a extensão do FDW
CREATE EXTENSION postgres_fdw;

--mostra se já existe algum servidor externo já configurado
select 
    srvname as name, 
    srvowner::regrole as owner, 
    fdwname as wrapper, 
    srvoptions as options
from pg_foreign_server
join pg_foreign_data_wrapper w on w.oid = srvfdw;

-- cria um servidor externo
CREATE SERVER Nome_Da_Conexao -- vocë vai usar esse nome mais tarde para os mapeamentos
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'IP_DO_SERVIDOR_REMOTO OU FQDN', dbname 'NOME_DO_BANCO_REMOVO', port '5432'); -- aqui fica uma dica, pelo menos na GCP mesmo que o banco seja no mesmo servidor precisa ser colocado o ip da LAN, não da pra usar localhost 

-- caso vc queira apagar o servidor externo
DROP SERVER Nome_Da_Conexao cascade;

-- mapeamento do usuário com permissão na outra base
CREATE USER MAPPING FOR o_usuário_da_conexão
SERVER Nome_Da_Conexao
OPTIONS (user 'o_usuário_remoto', password 'a_senha_complexa');

-- para esse exemplo, vou criar um schema e "importar" a estrutura de objetos do public lá do outro banco para dentro desse schema local do banco onde vamos rodar nossas querys
create schema remoto;

-- aqui fazemos a importação dos objetos, basicamente ele vai criar um monte de tabela extrangeira que é basicamente a estrutura de metadados, não importa nenhum dado em sí
IMPORT FOREIGN SCHEMA public
FROM SERVER Nome_Da_Conexao
INTO remoto;

-- caso queia importar um objeto específico ao invés de todos os objetos
IMPORT FOREIGN SCHEMA public LIMIT TO (tbl_teste123)
    FROM SERVER Nome_Da_Conexao INTO remoto;

-- por padrão o FDW trabalha com fetch de dados (trazer dados de um banco para outro) de 100 em 100 registros, isso pode ser muito demorado dependendo do volume de dados e complexidade de query

-- vamos alterar para 1000000 pela primeira vez
ALTER SERVER Nome_Da_Conexao OPTIONS (fetch_size '1000000');

-- caso queia alterar o valo que vc alterou para outro valor, o comando muda
ALTER SERVER Nome_Da_Conexao OPTIONS (set fetch_size '1000000');

-- aqui tentamos fazer com que a query que vai ser executada remotamente tente usar as estatísticas do banco de lá, índices, etc.
ALTER SERVER Nome_Da_Conexao OPTIONS (add use_remote_estimate 'true');

-- essas duas opções ajudam caso você tente operar DML remotamente
alter server Nome_Da_Conexao options (add async_capable 'true');
alter server Nome_Da_Conexao options (add parallel_commit 'true');

-- aqui é para o uso de estatísticas remotas
alter server Nome_Da_Conexao options (add analyze_sampling 'bernoulli'); --random ?


 -- trocar a senha
ALTER USER MAPPING FOR usuário SERVER Nome_Da_Conexao OPTIONS (SET password 'a nova senha');

-- para dar permissão para os objetos que estão locais para o usário da aplicação acessar o objeto remoto
GRANT USAGE ON FOREIGN SERVER Nome_Da_Conexao TO usuário_da_aplicacao;

-- legal mas isso agora significa que? se você está em um banco A e precisa acessar tabela em um banco B dessa forma o select ficaria assim:
select * from remoto.tabela;

-- simples, bonito, sexy, sem precisar mapear colunas, passar usuário e senha, ou criar uma conexão dblink monstro para um simples select.
