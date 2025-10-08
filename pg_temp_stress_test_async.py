#!/usr/bin/env python3
"""
pg_temp_stress_test_async.py — PostgreSQL temp table stress test (async version only)

Melhorias:
  ✓ Execução totalmente assíncrona via asyncpg
  ✓ Finalização automática ao fim do tempo definido
  ✓ CSV detalhado com histórico de métricas acumuladas
  ✓ Métricas médias de tempo de criação e drop de tabelas temporárias

Dependências:
  pip install asyncpg

Exemplo de uso:
  python3 pg_temp_stress_test_async.py --host 127.0.0.1 --port 5432 --dbname testdb --user postgres --password 1234 \
    --max-conns 50 --test-duration 120 --rows-per-table 5000 --create-delay 0.1 --csv-report report.csv
"""

import argparse
import asyncio
import asyncpg
import random
import time
import csv
import uuid
import signal
import sys
from datetime import datetime, timedelta

STOP_REQUESTED = False

def handle_signal(sig, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    print(f"\nReceived signal {sig}, stopping...")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

async def async_worker(conn_info, worker_id, rows_per_table, mean_delay, do_select, stats):
    global STOP_REQUESTED
    try:
        conn = await asyncpg.connect(**conn_info)
        while not STOP_REQUESTED:
            tbl = f"tmp_{worker_id}_{uuid.uuid4().hex[:8]}"
            try:
                # medir tempo de criação
                t0 = time.perf_counter()
                await conn.execute(f"CREATE TEMP TABLE {tbl} AS SELECT i AS n, md5(i::text) AS v FROM generate_series(1, $1) i", rows_per_table)
                create_time = (time.perf_counter() - t0) * 1000
                stats['created'] += 1
                stats['total_create_time_ms'] += create_time

                if do_select:
                    await conn.fetchval(f"SELECT count(*) FROM {tbl}")

                # medir tempo de drop
                t1 = time.perf_counter()
                await conn.execute(f"DROP TABLE IF EXISTS {tbl}")
                drop_time = (time.perf_counter() - t1) * 1000
                stats['dropped'] += 1
                stats['total_drop_time_ms'] += drop_time

            except Exception as e:
                stats['errors'] += 1
                print(f"[worker {worker_id}] error: {e}", file=sys.stderr)

            await asyncio.sleep(random.expovariate(1.0 / max(mean_delay, 0.001)))
    except Exception as e:
        stats['errors'] += 1
        print(f"[worker {worker_id}] connection error: {e}", file=sys.stderr)
    finally:
        try:
            await conn.close()
        except Exception:
            pass

async def run_async(args):
    global STOP_REQUESTED
    conn_info = dict(user=args.user, password=args.password, database=args.dbname, host=args.host, port=args.port)
    stats = {
        'created': 0,
        'dropped': 0,
        'errors': 0,
        'total_create_time_ms': 0.0,
        'total_drop_time_ms': 0.0
    }

    csv_file = open(args.csv_report, 'w', newline='') if args.csv_report else None
    csv_writer = None
    if csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'elapsed_s', 'created', 'dropped', 'errors', 'avg_create_ms', 'avg_drop_ms'])

    start = datetime.now()
    end = start + timedelta(seconds=args.test_duration)

    async def monitor():
        while datetime.now() < end and not STOP_REQUESTED:
            await asyncio.sleep(args.log_interval)
            elapsed = (datetime.now() - start).total_seconds()

            avg_create = (stats['total_create_time_ms'] / stats['created']) if stats['created'] > 0 else 0
            avg_drop = (stats['total_drop_time_ms'] / stats['dropped']) if stats['dropped'] > 0 else 0

            msg = (f"[{datetime.now().isoformat()}] created={stats['created']} dropped={stats['dropped']} "
                   f"errors={stats['errors']} avg_create={avg_create:.2f}ms avg_drop={avg_drop:.2f}ms elapsed={elapsed:.1f}s")
            print(msg)

            if csv_writer:
                csv_writer.writerow([datetime.now().isoformat(), f"{elapsed:.1f}", stats['created'], stats['dropped'], stats['errors'], f"{avg_create:.2f}", f"{avg_drop:.2f}"])
                csv_file.flush()

    monitor_task = asyncio.create_task(monitor())

    workers = []
    for i in range(args.max_conns):
        if STOP_REQUESTED:
            break
        task = asyncio.create_task(async_worker(conn_info, f"w{i+1}", args.rows_per_table, args.create_delay, args.select_after_create, stats))
        workers.append(task)
        await asyncio.sleep(args.ramp_interval)

    # Esperar até o tempo de teste acabar
    while datetime.now() < end and not STOP_REQUESTED:
        await asyncio.sleep(0.5)

    STOP_REQUESTED = True
    print("Finalizando workers...")

    # Cancelar tarefas e aguardar encerramento
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    await monitor_task

    avg_create = (stats['total_create_time_ms'] / stats['created']) if stats['created'] > 0 else 0
    avg_drop = (stats['total_drop_time_ms'] / stats['dropped']) if stats['dropped'] > 0 else 0

    print("\nFinal stats:")
    print(f"created={stats['created']} dropped={stats['dropped']} errors={stats['errors']}")
    print(f"avg_create={avg_create:.2f}ms avg_drop={avg_drop:.2f}ms")

    if csv_writer:
        csv_writer.writerow([datetime.now().isoformat(), f"{(datetime.now()-start).total_seconds():.1f}", stats['created'], stats['dropped'], stats['errors'], f"{avg_create:.2f}", f"{avg_drop:.2f}"])
        csv_file.flush()
        csv_file.close()
        print(f"Report saved to {args.csv_report}")


def parse_args():
    p = argparse.ArgumentParser(description='Async stress test for Postgres temp tables')
    p.add_argument('--host', required=True)
    p.add_argument('--port', type=int, default=5432)
    p.add_argument('--dbname', required=True)
    p.add_argument('--user', required=True)
    p.add_argument('--password', required=True)
    p.add_argument('--max-conns', type=int, default=10)
    p.add_argument('--ramp-interval', type=float, default=0.5)
    p.add_argument('--test-duration', type=int, default=60)
    p.add_argument('--rows-per-table', type=int, default=1000)
    p.add_argument('--create-delay', type=float, default=0.2)
    p.add_argument('--select-after-create', action='store_true')
    p.add_argument('--log-interval', type=int, default=5)
    p.add_argument('--csv-report', help='CSV file to write results')
    return p.parse_args()

if __name__ == '__main__':
    args = parse_args()
    asyncio.run(run_async(args))
