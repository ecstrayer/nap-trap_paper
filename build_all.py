import os 
import sys
import toml
import src.database.build_db as build_db
import src.database.mpra_db as mpra_db


def build_from_toml(build_path):

    params = toml.load(build_path)

    db_path = params['paths']['db_path']
    schema_path = params['paths']['schema_path']
    output_path = params['paths']['output_path']

    if not os.path.exists(output_path):
        os.makedirs(os.path.join(output_path,'tmp'))
    
    if not os.path.exists(db_path):
        db = mpra_db.MPRA_DB(db_path = db_path, output_path = output_path, schema_path = schema_path)
        db = build_db.make_db(db, params)
    
    else:
        db = mpra_db.MPRA_DB(db_path = db_path, output_path = output_path)



def main():
    kozak_db = build_from_toml('libraries/kozak/build_db.toml')
    utr5_fish_db = build_from_toml('libraries/utr5_fish/build_db.toml')
    validation_db = build_from_toml('libraries/utr5_fish/build_db.toml')
    multiframe_db = build_from_toml('libraries/multiframe/build_db.toml')