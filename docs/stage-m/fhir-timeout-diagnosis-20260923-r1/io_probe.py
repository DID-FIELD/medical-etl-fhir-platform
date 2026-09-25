import json, sqlite3, tempfile, time, os
from pathlib import Path
root=Path('/mnt/f/project/medical-etl-fhir-platform')
out=root/'output/fhir-timeout-diagnosis-20260923-r1'
source=next((root/'output/full-consumers-p10000-r2/results').glob('*/fhir/attempt-0001/fhir/Encounter.ndjson'))
report={'purpose':'Same SQLite inserts/cache on DrvFS versus native Linux temporary storage','cases':[]}
for label,parent in [('drvfs',out),('linux',Path('/tmp'))]:
    with tempfile.TemporaryDirectory(prefix='fhir-io-probe-',dir=parent) as scratch:
        db=sqlite3.connect(str(Path(scratch)/'index.sqlite'))
        db.execute('PRAGMA cache_size=-8192')
        db.execute('PRAGMA temp_store=FILE')
        db.execute('CREATE TABLE resources(kind TEXT,id TEXT,patient TEXT,encounter TEXT,body TEXT,PRIMARY KEY(kind,id))')
        start=time.perf_counter(); n=0
        with source.open() as stream:
            for line in stream:
                r=json.loads(line)
                db.execute('INSERT INTO resources VALUES (?,?,?,?,?)',('Encounter',r['id'],r['subject']['reference'],None,line))
                n+=1
                if n>=100000 or (n%1000==0 and time.perf_counter()-start>45): break
        elapsed=time.perf_counter()-start
        report['cases'].append({'storage':label,'rows':n,'seconds':elapsed,'rows_per_second':n/elapsed,'sqlite_version':sqlite3.sqlite_version,'filesystem_device':os.stat(scratch).st_dev})
        db.close()
        print(json.dumps(report['cases'][-1]),flush=True)
(out/'io-probe.json').write_text(json.dumps(report,indent=2))
