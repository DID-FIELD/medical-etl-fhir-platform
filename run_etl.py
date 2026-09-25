"""Run the Synthea snapshot pipeline, optionally publishing to the project database."""
import argparse
import json


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',default='data/external/synthea_csv.zip')
    parser.add_argument('--output-root',default='output/synthea')
    parser.add_argument('--run-id')
    parser.add_argument('--publish-db',action='store_true')
    parser.add_argument('--legacy-emr',action='store_true',help='Run the original EMR demo using its legacy database settings')
    args=parser.parse_args()
    if args.legacy_emr:
        if args.publish_db:parser.error('--legacy-emr cannot be combined with --publish-db')
        from src.pipeline import run_full_etl
        run_full_etl();return
    from src.synthea_pipeline import run
    result=run(args.archive,args.output_root,args.run_id)
    print(json.dumps({k:result[k] for k in ['run_id','status','counts','quality','elapsed_seconds']},indent=2))
    if args.publish_db:
        from pathlib import Path
        from src.database.synthea_store import load_run
        print(json.dumps(load_run(Path(args.output_root)/'runs'/result['run_id']),indent=2))


if __name__=='__main__':main()
