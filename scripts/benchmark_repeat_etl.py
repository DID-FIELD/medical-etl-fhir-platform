"""Repeat only ETL on the same archive; avoid concurrent generation for timing."""
import argparse,json,sys
from pathlib import Path
from scripts.benchmark_synthea import ROOT,measured,save

p=argparse.ArgumentParser(description=__doc__);p.add_argument('--population',type=int,required=True);p.add_argument('--repeat',type=int,required=True)
a=p.parse_args();n=a.population
archive=ROOT/f'data/generated/scale-v4-p{n}/source.zip'
run_id=f'p{n}-r{a.repeat}'
if (ROOT/f'output/scale-v4/runs/{run_id}').exists():raise FileExistsError(run_id)
r=measured([sys.executable,'-m','src.synthea_pipeline','--archive',str(archive),'--output-root',str(ROOT/'output/scale-v4'),'--run-id',run_id],ROOT/f'data/generated/scale-v4-p{n}/etl-r{a.repeat}.log')
manifest=ROOT/f'output/scale-v4/runs/{run_id}/manifest.json'
result={'population_requested':n,'repeat':a.repeat,'measurement':r,'manifest':json.loads(manifest.read_text()) if manifest.exists() else None}
if result['manifest'] and result['manifest']['status']=='SUCCESS':
 original=json.loads((ROOT/f'output/scale-v4/runs/p{n}-r1/manifest.json').read_text())
 result['same_counts_as_first']=original['counts']==result['manifest']['counts']
 result['same_json_artifacts_as_first']=all(h==result['manifest']['artifacts'][f] for f,h in original['artifacts'].items() if f.endswith('.json'))
save(ROOT/f'docs/stage-e/p{n}-r{a.repeat}.json',result)
print(json.dumps({k:v for k,v in result.items() if k!='manifest'},indent=2))
if r['exit_code']!=0:raise SystemExit(1)
