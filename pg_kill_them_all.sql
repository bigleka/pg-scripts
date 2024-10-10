/*
Matar todos os processos que estão bloqueando seu processo.
Caso esteja fazendo alguma manutenção no ambiente e precisa garantir que ninguém atrapalhe.
em uma sessão você executa o que precisa e em outra você sai matando todos que entrarem na sua frente.
*/
DO $$
DECLARE
    blocking_pids INT[];
    pid_to_kill INT := SEU_PID_AQUI -- Substitua SEU_PID_AQUI pelo PID específico
    current_pid INT;
BEGIN
    SELECT pg_blocking_pids(pid) INTO blocking_pids
    FROM pg_stat_activity
    WHERE pid = pid_to_kill;

    FOREACH current_pid IN ARRAY blocking_pids
    LOOP
        PERFORM pg_terminate_backend(current_pid);
        RAISE NOTICE 'Processo com PID % finalizado.', current_pid;
    END LOOP;
END $$;
