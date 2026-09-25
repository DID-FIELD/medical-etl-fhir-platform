"""Small synthetic fixtures without API or database imports."""
import csv
import io
import zipfile
from src.synthea_pipeline import REQUIRED

def fixture_sources():
    p=[dict(Id='p1',BIRTHDATE='1980-01-01',DEATHDATE='',GENDER='M'),
       dict(Id='p2',BIRTHDATE='1981-01-01',DEATHDATE='',GENDER='F')]
    e=[dict(Id='e1',PATIENT='p1',START='2024-01-01T00:00:00Z',STOP='2024-01-02T00:00:00Z',ENCOUNTERCLASS='ambulatory',CODE='001',DESCRIPTION='Test')]
    def image(instance,series,modality):
        return dict(Id='s1',PATIENT='p1',ENCOUNTER='e1',DATE='2024-01-01T01:00:00Z',SERIES_UID=series,INSTANCE_UID=instance,
                    MODALITY_CODE=modality,BODYSITE_CODE='123',BODYSITE_DESCRIPTION='Body',SOP_CODE='1.2.3')
    return {'patients':p,'encounters':e,'imaging_studies':[image('i1','se1','CT'),image('i2','se1','CT'),image('i3','se2','MR')]}

def archive(tmp_path,sources,name='source.zip'):
    path=tmp_path/name
    with zipfile.ZipFile(path,'w') as z:
        for table,rows in sources.items():
            body=io.StringIO();w=csv.DictWriter(body,fieldnames=REQUIRED[table]);w.writeheader();w.writerows(rows)
            z.writestr(table+'.csv',body.getvalue())
    return path
