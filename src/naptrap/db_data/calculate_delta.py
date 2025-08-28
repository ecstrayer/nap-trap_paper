import numpy as np


def calculate_delta(db, sample_name, num_run_type, denom_run_type, data_type):

    '''
    If numerator and demonator are 0 do not record
    else nan == -1
    '''

    num_run_ids = db.select(columns = ['replicate_id','run_id']).where((db['sample_name'] == sample_name) & (db['run_type'] == num_run_type)).to_dict()
    denom_run_ids = db.select(columns = ['replicate_id','run_id']).where((db['sample_name'] == sample_name) & (db['run_type'] == denom_run_type)).to_dict()
    selected_runs = list(num_run_ids.values()) + list(denom_run_ids.values())

    ridx, cidx, X = db.select(columns = ['reporter_id','run_id','normalized_count']).where((db['sample_name'] == sample_name) & (db['run_id'].in_(selected_runs))).to_numpy(column_ids = selected_runs)
    processed_data = []
    data_attribute = []
    run_to_data = []
    max_data_id = db.get_max('data_id')


    for k in num_run_ids.keys():
        max_data_id += 1
        run_ids = [num_run_ids[k], denom_run_ids[k]]
        run_to_data += [tuple([r, max_data_id]) for r in run_ids]

        data_info = f'calculate_delta(sample_name = {sample_name}, num_run_type = {num_run_type}, denom_run_type  = {denom_run_type}, data_type = {data_type})'
        data_attribute.append(tuple([max_data_id, k, data_type, data_info]))
        nidx = cidx[num_run_ids[k]]
        didx = cidx[denom_run_ids[k]]
        delta = X[:,nidx] / X[:,didx] 
        for i,d in enumerate(delta):
            if np.isnan(d) or np.isinf(d):
                if X[i,didx] == 0:
                    continue
                else:
                    d == -1

            processed_data.append(tuple([max_data_id,i,d]))
    
    db.insert(table_name= 'processed_data_iso', columns = ['data_id','reporter_id', 'processed_data_value'], values = processed_data)
    db.insert(table_name= 'data_attribute', columns = ['data_id', 'replicate_id','data_type', 'data_info'], values = data_attribute)
    db.insert(table_name= 'run_to_data_iso', columns = ['run_id','data_id'], values = run_to_data)
    
    data_ids_added = [d[0] for d in data_attribute]
    return data_ids_added


def mean_delta(db, data_ids_added):


    replicate_number = db.select(columns = ['sample_id']).where(db['data_id'].in_(data_ids_added)).groupby([db['sample_id']], aggregate_functions = {'count' : ['data_id']}).to_dict()
    max_data_group = db.get_max('data_group_id')
    group_lookup = db.select(['data_group_name','data_group_id']).where(db['data_id'].in_(data_ids_added)).to_dict()
    sample_data = []
    data_group = []
    data_group_to_data = []
    
    output = db.select(['sample_name','sample_id', 'data_type', 'reporter_id']).groupby([db['sample_id'],db['reporter_id'], db['data_type']], aggregate_functions= {'AVG':['processed_data_value'],'group_concat':['data_id']}).where(db['data_id'].in_(data_ids_added)).fetchall()

    for sample_name, sample_id, data_type, reporter_id, mean_delta, data_ids in output:
        data_ids = [int(d) for d in sorted(data_ids.split(','))]
        data_group_name = f'{sample_name}-{data_type}'
        if len(data_ids) != replicate_number[sample_id]:
            continue
        if data_group_name not in group_lookup:
            max_data_group += 1
            group_lookup[data_group_name] = max_data_group
            data_group.append(tuple([max_data_group, sample_id, f'mean_{data_type}', data_group_name]))
            for d in data_ids:
                data_group_to_data.append(tuple([max_data_group,d]))
        
        group_id = group_lookup[data_group_name]
        sample_data.append([group_id, reporter_id, mean_delta])

    group_ids_added = [g[0] for g in data_group]

    db.insert(table_name = 'sample_data_iso', columns = ['data_group_id', 'reporter_id', 'data_value'], values = sample_data)
    db.insert(table_name = 'data_group_attribute_iso', columns = ['data_group_id', 'sample_id', 'data_group_type','data_group_name'], values = data_group)
    db.insert(table_name = 'data_group_to_data_iso', columns = ['data_group_id','data_id'], values = data_group_to_data)


    return group_ids_added


def to_db(db, params):

    samples = params['samples']
    num_run_type = params['num_run_type']
    denom_run_type = params['denom_run_type']
    data_type = params['data_type']
    data_ids_added = []

    for sample_name in samples:
        data_ids_added += calculate_delta(db, sample_name= sample_name, num_run_type= num_run_type, denom_run_type = denom_run_type, data_type = data_type)

    data_group_ids_added = mean_delta(db,data_ids_added)

    