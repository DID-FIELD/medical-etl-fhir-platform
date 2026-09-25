import json
import pandas as pd
import pytest
from scripts.prepare_datasets import generate
from src.audited_pipeline import run_audited_etl, prepare_records
from src.fhir.converter import convert_to_fhir_observation
from src.processing.cleaner import standardize_fields


def sample(**changes):
    return dict(patient_id='001', patient_name='Test', gender='M', birth_date='1980-01-01',
                exam_type='CT', study_date='20240101', **changes)


def test_cleaning_idempotent_and_missing_id_preserved():
    df = pd.DataFrame([sample(), {**sample(), 'patient_id': '', 'study_date':'2024-01-02'}])
    first=standardize_fields(df,'emr')
    pd.testing.assert_frame_equal(first, standardize_fields(first,'emr'))
    assert pd.isna(first.loc[1,'patient_id'])
    assert first.study_date.tolist() == ['2024-01-01','2024-01-02']


def test_two_studies_same_patient_date_type_are_retained_with_source_ids():
    rows=pd.DataFrame([{**sample(),'source_record_id':'study-1'}, {**sample(),'source_record_id':'study-2'}])
    assert prepare_records(rows).disposition.tolist()==['accepted','accepted']


def test_conflicting_patient_attributes_and_source_event_are_quarantined():
    rows=pd.DataFrame([sample(), {**sample(), 'gender':'F'}])
    assert prepare_records(rows).disposition.eq('quarantined').all()
    rows=pd.DataFrame([{**sample(), 'source_record_id':'same'}, {**sample(), 'exam_type':'MR','source_record_id':'same'}])
    assert prepare_records(rows).reason.str.contains('conflicting_source_record_id').all()


def test_controlled_anomalies_reconcile_and_rerun(tmp_path):
    dataset=tmp_path/'input'
    spec=generate(1000,dataset,dirty=True)
    one=run_audited_etl(dataset/'emr.csv',tmp_path/'runs','first')
    two=run_audited_etl(dataset/'emr.csv',tmp_path/'runs','second')
    for key,value in spec['expected'].items():
        assert one['counts'][key]==value
    assert one['counts']==two['counts']
    assert all(one['checks'].values())
    for name in ['patient.json','observation.json','dim_patient.parquet','dws_patient_exam_summary.parquet']:
        assert one['artifacts'][name]['sha256']==two['artifacts'][name]['sha256']
    with pytest.raises(FileExistsError):
        run_audited_etl(dataset/'emr.csv',tmp_path/'runs','first')
    assert json.loads((tmp_path/'runs/first/manifest.json').read_text())['status']=='SUCCESS'


def test_failure_manifest_and_no_success_on_bad_schema(tmp_path):
    path=tmp_path/'bad.csv'; path.write_text('x\n1\n')
    with pytest.raises((KeyError,ValueError)):
        run_audited_etl(path,tmp_path/'runs','failure')
    assert json.loads((tmp_path/'runs/failure/manifest.json').read_text())['status']=='FAILED'


def test_empty_input_and_all_rejected(tmp_path):
    for name, rows in [('empty',[]),('invalid',[{**sample(),'study_date':'garbage'}])]:
        path=tmp_path/f'{name}.csv'
        pd.DataFrame(rows,columns=list(sample())).to_csv(path,index=False)
        result=run_audited_etl(path,tmp_path/'runs',name)
        assert result['counts']['accepted']==0
        assert all(result['checks'].values())


def test_observations_unique_and_json_serializable():
    one=convert_to_fhir_observation({'patient_id':'PID-A','exam_type':'CT','study_date':'2024-01-01'})
    two=convert_to_fhir_observation({'patient_id':'PID-A','exam_type':'MR','study_date':'2024-01-01'})
    assert one['id']!=two['id']
    assert json.loads(json.dumps(one))['effectiveDateTime']=='2024-01-01'


def test_leading_zero_ids_preserved(tmp_path):
    path=tmp_path/'data.csv'; pd.DataFrame([sample()]).to_csv(path,index=False)
    run_audited_etl(path,tmp_path/'runs','zeros')
    assert pd.read_parquet(tmp_path/'runs/zeros/ods_emr_raw.parquet').patient_id.tolist()==['001']
