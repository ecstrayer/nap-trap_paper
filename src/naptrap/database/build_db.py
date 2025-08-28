import argparse
import toml
import pyfaidx
import warnings

import src.database.mpra_db as mpra_db
import src.db_features as db_features
import src.db_data as db_data
import src.db_analysis as db_analysis

#functions to add features to the database


def add_reporters(db, fasta_path):

    '''
    Function to add reporter sequences to the database.
    '''

    reporter_fa = pyfaidx.Fasta(fasta_path)
    reporters = []

    for rid, (rname, rsequence) in enumerate(reporter_fa.items()):
        seq = rsequence[:].seq
        rname = rname.split('#')
        rtags = ','.join(rname[1:]) 
        rname = rname[0]
        reporters.append([rid,rname,seq] + [rtags])

    db.insert(table_name = 'reporter', columns = ['reporter_id','reporter_name','insert_sequence','tags'], values = reporters)



def add_constants(db, constant_params):

    c = db.constants if 'mpra_constant' in db.tables else dict()
    c = {}
    cdata = [tuple([cname,value]) for cname,value in constant_params.items() if cname not in c]

    db.insert(table_name = 'mpra_constant', columns = ['constant_name','constant_value'], values = cdata)

def add_features(db, feature_params):
    
    for feature, arg_list in feature_params.items():
        for args in arg_list:
            if feature not in db_features.__all__:
                raise Exception(f'Please supply a valid feature function. {feature} not found in src/db_features/')

            feature_module = getattr(db_features, feature)
            try:
                feature_module.to_db(db, args)
            except:
                raise Exception('All feature_modules need a to_db(db,params) function')


def add_data(db, data_params):

    db_data.add_data.add_data(db = db, data_params = data_params, add_to_db = True)

    for data_function, args_list in data_params['functions'].items():
        for args in args_list:
            if data_function not in db_data.__all__:
                raise Exception(f'Please supply a valid feature function. {data_function} not found in src/db_function/')
            data_module = getattr(db_data, data_function)
            try:
                data_module.to_db(db, args)
            except:
                raise Exception('All data_modules need a to_db(db,params) function')


def add_analysis(db, analysis_params):


    for analysis, arg_list in analysis_params.items():
        for args in arg_list:
            if analysis not in db_analysis.__all__:
                raise Exception(f'Please supply a valid feature function. {analysis} not found in src/db_analysis/')

            analysis_module = getattr(db_analysis, analysis)
            try:
                analysis_module.to_db(db, args)
            except:
                raise Exception('All analysis_modules need a to_db(db,params) function')


def make_db(db, params):


    if db.reporter_num is None:
        fasta_path = params['paths']['fasta_path']
        add_reporters(db,fasta_path = fasta_path)
        add_constants(db, params['constants'])

    if 'data' in params:
        add_data(db, params['data'])
    else:
        warnings.warn('data section not found in params.')

    if 'features' in params:
        add_features(db, params['features'])
    else:
        warnings.warn('feature section not found in params.')

    if 'selector_path' in params['paths'] and 'analyses' in params:
        selector_path = params['paths']['selector_path']
        db.add_selectors(selector_path)
        db.load_features('all')
        add_analysis(db, params['analyses'])

    else:
        warnings.warn('analyses section or selector_path not found in params.')

    return db



def main():

    parser = argparse.ArgumentParser(description = 'Build an MPRA database.')
    parser.add_argument('--toml_path', help = 'Path to build toml')
    args = parser.parse_args()

    params = toml.load('libraries/utr5_fish/build_db.toml')

    db_path = params['paths']['db_path']
    schema_path = params['paths']['schema_path']
    output_path = params['paths']['output_path']

    db = mpra_db.MPRA_DB(db_path = db_path, output_path = output_path, schema_path = schema_path)
    db = make_db(db, params)




if __name__ == '__main__':
    main()
    





