import json, sys, time, sqlite3, tempfile
from pathlib import Path
root=Path('/mnt/f/project/medical-etl-fhir-platform')
sys.path.insert(0,str(root))
from src.consumer_workflow import run_attempt, code_hashes
from src.spark_full_workflow import verify_inputs, verify_result
from src.stream_io import file_hash
out=root/'output/fhir-timeout-diagnosis-20260923-r1'
source=next((root/'output/full-consumers-p10000-r2/results').glob('*/spark-full/attempt-0001/manifest.json'))
meta=json.loads(source.read_text()); run_id=meta['run_id']
archive=root/'data/generated/scale-v4-p10000/source.zip'
baseline=root/'output/scale-stream-final/runs/p10000-stream-local-r1'
reference=root/'output/fhir/p10000-stream-r2'
report={'status':'RUNNING','run_id':run_id,'source_manifest_sha256':file_hash(source),'code_sha256':code_hashes(),'scratch_parent':tempfile.gettempdir()}
def save(): (out/'independent-export.json').write_text(json.dumps(report,indent=2))
save()
try:
    a,b=verify_inputs(archive,baseline,'+08:00')
    verify_result(source.parent,run_id,a,b,'+08:00')
    started=time.perf_counter()
    receipt=Path(run_attempt(source,out/'results',run_id,'fhir',1))
    report['consumer_seconds']=time.perf_counter()-started
    report['manifest']=str(receipt); report['manifest_sha256']=file_hash(receipt)
    print('EXPORT_SUCCESS seconds='+str(report['consumer_seconds']),flush=True)
    save()
    before=receipt.read_bytes()
    assert Path(run_attempt(source,out/'results',run_id,'fhir',2))==receipt
    assert receipt.read_bytes()==before
    assert not (receipt.parent.parent/'attempt-0002').exists()
    report['retry_reused']=True
    expected=json.loads((reference/'manifest.json').read_text())
    assert expected['status']=='SUCCESS' and expected['source_manifest_sha256']==b
    report['comparisons']={}
    with tempfile.TemporaryDirectory(prefix='fhir-equivalence-') as scratch:
        db=sqlite3.connect(str(Path(scratch)/'compare.sqlite'))
        try:
            db.execute('PRAGMA cache_size=-8192')
            db.execute('CREATE TABLE rows(id TEXT PRIMARY KEY,body TEXT,seen INTEGER DEFAULT 0)')
            for kind in ['Patient','Encounter','ImagingStudy']:
                name=kind+'.ndjson'
                assert file_hash(reference/name)==expected['artifacts'][name]
                db.execute('DELETE FROM rows')
                with (reference/name).open() as stream:
                    for line in stream:
                        row=json.loads(line)
                        db.execute('INSERT INTO rows(id,body) VALUES (?,?)',(row['id'],json.dumps(row,sort_keys=True,separators=(',',':'))))
                n=0
                with (receipt.parent/'fhir'/name).open() as stream:
                    for line in stream:
                        row=json.loads(line)
                        assert db.execute('UPDATE rows SET seen=1 WHERE id=? AND body=? AND seen=0',(row['id'],json.dumps(row,sort_keys=True,separators=(',',':')))).rowcount==1
                        n+=1
                assert n==expected['counts'][kind]
                assert db.execute('SELECT 1 FROM rows WHERE seen=0 LIMIT 1').fetchone() is None
                report['comparisons'][kind]={'rows':n,'missing':0,'extra':0}
                print('EQUIVALENT '+kind+' '+str(n),flush=True)
                save()
        finally: db.close()
    assert code_hashes()==report['code_sha256']
    assert file_hash(source)==report['source_manifest_sha256']
    report['status']='SUCCESS'; save()
except BaseException as exc:
    report.update(status='FAILED',error_type=type(exc).__name__); save(); raise
