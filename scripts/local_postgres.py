"""Manage a project-owned PostgreSQL cluster without changing the system cluster."""
import argparse
import json
from pathlib import Path
import secrets
import subprocess
import tempfile

import psycopg2
from psycopg2 import sql

ROOT=Path(__file__).resolve().parents[1]
HOME=ROOT/'output'/'local-postgres'
CONFIG=HOME/'connection.json'


def command(binary, *args):
    # PostgreSQL children may inherit pipe handles; use files so a started server
    # cannot keep subprocess.communicate() waiting indefinitely.
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        result=subprocess.run([str(binary),*map(str,args)],stdout=stdout,stderr=stderr,timeout=90)
        if result.returncode:
            stderr.seek(0);stdout.seek(0)
            raise RuntimeError(stderr.read().decode('utf-8',errors='replace') or stdout.read().decode('utf-8',errors='replace'))
        return result


def manage(action, pg_bin=r'C:\Program Files\PostgreSQL\18\bin', port=55432):
    binaries=Path(pg_bin)
    HOME.mkdir(parents=True,exist_ok=True)
    data=HOME/'data'
    if action=='init':
        if CONFIG.exists():
            manage('start',pg_bin,port);return
        if data.exists() and any(data.iterdir()):raise RuntimeError('Existing cluster found without config; refusing to overwrite')
        config={'host':'127.0.0.1','port':port,'user':'medical_local','password':secrets.token_urlsafe(32),'dbname':'medical_etl',
                'connect_timeout':5}
        password_file=HOME/'init-password.txt'
        password_file.write_text(config['password'],encoding='utf-8')
        command(binaries/'initdb.exe','-D',data,'-U',config['user'],'--auth=scram-sha-256','--encoding=UTF8','--locale=C',f'--pwfile={password_file}')
        CONFIG.write_text(json.dumps(config,indent=2),encoding='utf-8')
        # Only remove the initialization copy inside this project-owned directory.
        password_file.unlink()
        with (data/'postgresql.conf').open('a',encoding='utf-8') as f:
            f.write(f"\n# Project local development cluster\nlisten_addresses = '127.0.0.1'\nport = {port}\n")
        manage('start',pg_bin,port)
        print('Project database created. Credentials saved locally; password not printed.')
    elif action=='start':
        config=json.loads(CONFIG.read_text(encoding='utf-8'))
        try:
            probe=psycopg2.connect(**{**config,'dbname':'postgres'})
            probe.close()
        except psycopg2.OperationalError:
            command(binaries/'pg_ctl.exe','-D',data,'-l',HOME/'server.log','-w','start')
        conn=psycopg2.connect(**{**config,'dbname':'postgres'})
        try:
            conn.autocommit=True
            with conn.cursor() as cur:
                cur.execute('SELECT 1 FROM pg_database WHERE datname=%s',(config['dbname'],))
                if not cur.fetchone():
                    cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(config['dbname'])))
        finally:conn.close()
        print('Project PostgreSQL is running on its independent port; project database is ready.')
    elif action=='stop':
        command(binaries/'pg_ctl.exe','-D',data,'-w','stop','-m','fast')
        print('Project PostgreSQL stopped.')
    elif action=='status':
        config=json.loads(CONFIG.read_text())
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT current_database(), version()')
                name,version=cur.fetchone()
                print(json.dumps({'database':name,'host':config['host'],'port':config['port'],'version':version,'connected':True}))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['init','start','stop','status'])
    parser.add_argument('--pg-bin',default=r'C:\Program Files\PostgreSQL\18\bin')
    parser.add_argument('--port',type=int,default=55432)
    manage(**vars(parser.parse_args()))
