"""Bounded Windows Synthea generation + isolated file-ETL scale measurement."""
import argparse
import csv
import ctypes
from ctypes import wintypes
from hashlib import sha256
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import zipfile
import psutil

ROOT=Path(__file__).resolve().parents[1]
class Memory(ctypes.Structure):
    _fields_=[('length',wintypes.DWORD),('load',wintypes.DWORD)]+[(n,ctypes.c_ulonglong) for n in ['total','available','page_total','page_available','virtual_total','virtual_available','extended']]

def available():
    m=Memory();m.length=ctypes.sizeof(m)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m)):raise ctypes.WinError()
    return m.available

def measured(command,log,limit_seconds=1800,rss_limit_bytes=int(3.5*1024**3)):
    start=time.perf_counter();peak=0;lowest=available();reason=None;known={}
    with log.open('w',encoding='utf-8') as stream:
        proc=subprocess.Popen(command,cwd=ROOT,stdout=stream,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW)
        parent=psutil.Process(proc.pid)
        def stop_tree():
            for child in reversed(list(known.values())):
                try:child.terminate()
                except psutil.NoSuchProcess:pass
            if proc.poll() is None:proc.terminate()
            proc.wait(timeout=30)
        try:
            while True:
                rss=0
                try:
                    for process in [parent,*parent.children(recursive=True)]:known[process.pid]=process
                except psutil.NoSuchProcess:pass
                for process in known.values():
                    try:rss+=process.memory_info().rss
                    except psutil.NoSuchProcess:pass
                peak=max(peak,rss)
                free=available();lowest=min(lowest,free)
                code=proc.poll()
                if code is not None:break
                if free<700*1024**2:reason='available_memory_below_700MiB'
                elif rss>rss_limit_bytes:reason=f'process_tree_rss_above_{rss_limit_bytes}_bytes'
                elif time.perf_counter()-start>limit_seconds:reason='time_limit'
                if reason:stop_tree();break
                time.sleep(.25)
        finally:
            if proc.poll() is None:stop_tree()
    return {'command':command,'exit_code':proc.returncode,'seconds':round(time.perf_counter()-start,3),'sampled_peak_tree_rss_bytes':peak,'memory_method':'sum of launcher and descendant RSS sampled every 250ms; shared pages may be counted twice','minimum_available_memory_bytes':lowest,'stop_reason':reason,'log':str(log.relative_to(ROOT))}


def save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--population',type=int,required=True)
    args=parser.parse_args();n=args.population
    if n not in (100,1000,10000):parser.error('Choose 100, 1000 or 10000')
    if shutil.disk_usage(ROOT).free<15*1024**3:raise RuntimeError('Less than 15GiB free disk')
    if available()<3*1024**3:raise RuntimeError('Less than 3GiB available memory before generation')
    target=ROOT/f'data/generated/scale-v4-p{n}';target.mkdir(parents=True,exist_ok=False)
    evidence=ROOT/f'docs/stage-e/p{n}.json'
    jar=ROOT/'data/external/synthea-generator/synthea-v4.0.0.jar'
    metadata=json.loads((jar.parent/'download.json').read_text())
    if sha256(jar.read_bytes()).hexdigest()!=metadata['sha256']:raise ValueError('Generator hash mismatch')
    config=target/'run.properties'
    config.write_text('\n'.join([
        'exporter.baseDirectory='+target.as_posix(), 'exporter.fhir.export=false',
        'exporter.csv.export=true','exporter.csv.included_files=patients.csv,encounters.csv,imaging_studies.csv',
        'exporter.csv.excluded_files=', 'exporter.csv.append_mode=false','exporter.years_of_history=10',
        'generate.thread_pool_size=2','generate.log_patients.detail=none',
    ])+'\n',encoding='utf-8')
    report={'status':'RUNNING','population_requested':n,'seed':20260920,'clinician_seed':20260920,
            'reference_date':'20260920','end_date':'20260920','state':'Massachusetts','version':metadata,
            'history_years':10,'csv_scope':['patients','encounters','imaging_studies'],
            'java_heap_limit':'2g','threads':2,'config':config.read_text(),'measurements':{}}
    save(evidence,report)
    command=['java','-Xmx2g','-jar',str(jar),'-p',str(n),'-s','20260920','-cs','20260920',
             '-r','20260920','-e','20260920','-c',str(config),'Massachusetts']
    report['measurements']['generation']=measured(command,target/'generation.log')
    save(evidence,report)
    if report['measurements']['generation']['exit_code']!=0:
        report['status']='GENERATION_FAILED';save(evidence,report);raise RuntimeError('Generation failed; see evidence/log')
    files=sorted((target/'csv').glob('*.csv'));inventory={}
    for file in files:
        with file.open(encoding='utf-8-sig',newline='') as stream:
            rows=csv.reader(stream);header=next(rows);count=sum(1 for _ in rows)
        inventory[file.stem]={'rows':count,'columns':len(header),'bytes':file.stat().st_size,'sha256':sha256(file.read_bytes()).hexdigest()}
    report['source']=inventory
    archive=target/'source.zip';started=time.perf_counter()
    with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for file in files:z.write(file,file.name)
    report['archive']={'bytes':archive.stat().st_size,'sha256':sha256(archive.read_bytes()).hexdigest(),'pack_seconds':round(time.perf_counter()-started,3)}
    output=ROOT/'output/scale-v4';run_id=f'p{n}-r1'
    report['measurements']['file_etl']=measured([sys.executable,'-m','src.synthea_pipeline','--archive',str(archive),'--output-root',str(output),'--run-id',run_id],target/'etl.log')
    report['status']='SUCCESS' if report['measurements']['file_etl']['exit_code']==0 else 'ETL_FAILED'
    manifest=output/'runs'/run_id/'manifest.json'
    if manifest.exists():report['etl_manifest']=json.loads(manifest.read_text(encoding='utf-8'))
    report['output_bytes']=sum(p.stat().st_size for p in (output/'runs'/run_id).rglob('*') if p.is_file()) if manifest.exists() else 0
    save(evidence,report)
    print(json.dumps({'population_requested':n,'status':report['status'],'source':inventory,'measurements':report['measurements']},indent=2))
    if report['status']!='SUCCESS':raise SystemExit(1)

if __name__=='__main__':main()
