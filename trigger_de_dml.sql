/*
este script cria a estrutura para monitorar operações de DML em nossas tabelas
*/

SET default_toast_compression=lz4;

CREATE TABLE tbl_audit_log (
    id serial PRIMARY KEY,
    currentdb text not null,
    table_name text NOT NULL,
    operation char(1) NOT NULL,
    old_data jsonb,
    new_data jsonb,
    executed_at timestamp default now(),
    username text,
    appname text,
    client_ip inet,
    commandquery text
);

--show all

CREATE OR REPLACE FUNCTION audit_log()
RETURNS TRIGGER AS $$
DECLARE
    current_ip inet;
    current_user_name text;
    current_app_name text;
    current_dml_query text;
    current_db text;
BEGIN
    SELECT current_setting('application_name') INTO current_app_name;
    SELECT inet_client_addr() INTO current_ip;
    select current_user into current_user_name;
    select current_query() into current_dml_query;
    select current_database() into current_db;

    IF (TG_OP = 'DELETE') THEN
        INSERT INTO tbl_audit_log (currentdb, table_name, operation, old_data, new_data, executed_at, username, appname, client_ip, commandquery)
        VALUES (current_db, TG_TABLE_NAME, 'D', row_to_json(OLD), NULL, now(), current_user_name, current_app_name, current_ip, current_dml_query);
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO tbl_audit_log (currentdb, table_name, operation, old_data, new_data, executed_at, username, appname, client_ip, commandquery)
        VALUES (current_db, TG_TABLE_NAME, 'U', row_to_json(OLD), row_to_json(NEW), now(), current_user_name, current_app_name, current_ip, current_dml_query);
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO tbl_audit_log (currentdb, table_name, operation, old_data, new_data, executed_at, username, appname, client_ip, commandquery)
        VALUES (current_db, TG_TABLE_NAME, 'I', NULL, row_to_json(NEW), now(), current_user_name, current_app_name, current_ip, current_dml_query);
        RETURN NEW;
    END IF;
    RETURN NULL; -- Default
END;
$$ LANGUAGE plpgsql;


-- depois de criar todos os objetos acima, criar a trigger para cada tabela que vai ser auditada
CREATE TRIGGER audit_log_tbl_teste123
AFTER INSERT OR UPDATE OR DELETE ON tbl_teste123
FOR EACH ROW
EXECUTE FUNCTION audit_log();
