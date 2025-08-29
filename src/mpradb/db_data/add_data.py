import json
import numpy as np
import warnings


def get_columns(db, table_name, tmp_dic, tmp_name):

    data_list = []

    for c in db.table_columns[table_name]:
        if c not in tmp_dic:
            raise Exception(f'Please provide {c} for {tmp_name}')
        data_list.append(tmp_dic[c])

    return data_list



def add_sample_info(db, sample_info):

    replicates_to_add = []
    runs_to_add = []
    samples_to_add = []
    run_ids_added = []
    sample_id = db.get_max('sample_id')
    run_id = db.get_max('run_id')
    replicate_id = db.get_max('replicate_id')
    sample_names_in_db = db.get_distinct(['sample_name'])
    run_names_in_db = db.get_distinct(['run_name'])


    replicate_lookup = {rname: rid for rname, rid in db.get_distinct(['replicate_name','replicate_id'])}

    for sample_name, sample_dic in sample_info.items():

        if sample_name not in sample_names_in_db:
            sample_id += 1

        sample_dic['sample_id'] = sample_id
        sample_dic['sample_name'] = sample_name
        tmp_sample = get_columns(db, 'sample_attribute', sample_dic, sample_name)

        samples_to_add.append(tmp_sample)

        for run_name, run_dic in sample_dic['runs'].items():
            if run_name in run_names_in_db:
                raise Exception(f'Run name {run_name} already added to the database.')

            tmp_replicate = []
            run_id += 1
            run_dic['run_name'] = run_name
            run_dic['run_id'] = run_id
            replicate_name = f'{sample_dic["experiment_name"]}-{sample_name}-{run_dic["replicate_name"]}'
            run_dic['replicate_name'] = replicate_name
            run_dic['sample_id'] = sample_id

            if replicate_name not in replicate_lookup:
                replicate_id += 1
                run_dic['replicate_id'] = replicate_id
                tmp_replicate = get_columns(db, 'replicate_attribute', run_dic, replicate_name)
                replicates_to_add.append(tmp_replicate)
                replicate_lookup[replicate_name] = replicate_id

            replicate_id = replicate_lookup[replicate_name]
            run_dic['replicate_id'] = replicate_id
            tmp_run = get_columns(db, 'run_attribute', run_dic, run_name)
            runs_to_add.append(tmp_run)
            run_ids_added.append(run_id)


    if len(samples_to_add) > 0:
        db.insert(table_name = 'sample_attribute', columns = db.table_columns['sample_attribute'], values = samples_to_add)

    if len(replicates_to_add) > 0:    
        db.insert(table_name = 'replicate_attribute', columns = db.table_columns['replicate_attribute'], values = replicates_to_add)
        
    if len(runs_to_add) > 0:
        db.insert(table_name = 'run_attribute', columns = db.table_columns['run_attribute'], values = runs_to_add)


    return run_ids_added



def add_data(db, data_params, add_to_db = False):
        
        ignore_not_found = data_params.get('ignore_not_found')
        ignore_not_found = False if ignore_not_found is None else ignore_not_found
        spike_ins = data_params.get('spike_ins')
        if spike_ins is None:
            spike_ins = {} 
            
        data_to_db = []

        run_ids_added = add_sample_info(db,data_params['samples'])

        data = {} 
        for exp_name, p in data_params['count_paths'].items():
            data = data | json.load(open(p,'r'))


        run_lookup = db.select(columns = ['run_name','run_id']).where(db['run_id'].in_(run_ids_added)).to_dict()
        run_to_sample = db.select(columns = ['run_name','sample_name']).where(db['run_id'].in_(run_ids_added)).to_dict()
        reporter_lookup = db.select(columns = ['reporter_name','reporter_id']).to_dict()

        max_reporter_id = db.get_max('reporter_id')
        

        for run_name, rcount in data.items():
            if run_name not in run_lookup:
                continue
                #warnings.warn(f'Run name: {run_name} not found in DB. Please supply annotation!')
            
            run_sp_to_exclude = spike_ins.get(run_to_sample[run_name])
            run_sp_to_exclude = [] if run_sp_to_exclude is None else run_sp_to_exclude.get('spike_ins_to_exclude')
            
            spike_idx = db.select(['reporter_id']).where((db['tags'] == 'spikein') & (db['reporter_name'].in_(run_sp_to_exclude, exclude = True))).fetchall()
            tmp_data = np.zeros([max_reporter_id+1, 3])
            run_id = run_lookup[run_name]

            for reporter_name, count in rcount.items():
                if reporter_name not in reporter_lookup:
                    if ignore_not_found:
                        continue
                    else:    
                        raise Exception(f'Reporter name: {reporter_name} not found in DB. Please check fasta!')
                
                rid = reporter_lookup[reporter_name]
                tmp_data[rid,0] = count
            
            tmp_data[:,1] = tmp_data[:,0] / tmp_data[:,0].sum()

            if len(spike_idx) > 0 and tmp_data[spike_idx].sum() > 0:
                reporter_mask = np.zeros(max_reporter_id+1)
                reporter_mask[spike_idx] = 1
                spike_sel = reporter_mask == 1
                tmp_data[:,2] = tmp_data[:,0] / tmp_data[spike_sel,0].sum()
            else:
                tmp_data[:,2] = tmp_data[:,1]

            for n in range(max_reporter_id + 1):
                rdata = tmp_data[n,:]
                if rdata.sum() > 0:
                    data_to_db.append([run_id, n, *rdata.tolist()])


        if add_to_db:
            db.insert(table_name = 'raw_data_iso', columns = ['run_id','reporter_id', 'raw_count', 'rpm','normalized_count'], values = data_to_db)
