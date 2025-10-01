
from random import sample
import sys 
import itertools
import numpy as np
import datetime
import os
import matplotlib.pyplot as plt
import seaborn as sns


def plot_replicate_heatmap(db,reporter_group_name,output_path, sample_names = None, fig_save_format = None):

    if fig_save_format == None:
        fig_save_format = 'svg'

    out_path = f'{output_path}/figures/replicate_comparision/{reporter_group_name}'

    if not os.path.exists(out_path):
        os.makedirs(out_path)


    
    
    reporter_group_id = db[['reporter_group_id']].where(db['reporter_group_name'] == reporter_group_name).fetchone()
    data_groups = set(db[['data_group_id']].where(db['reporter_group_id'] == reporter_group_id).to_list())

    reporter_group_samples  = list(set(db['sample_name'].where(db['data_group_id'].in_(data_groups) ).to_list()))

    if isinstance(sample_names,str):
        sample_names = [sample_names]

    if sample_names == None:

        sample_names = reporter_group_samples
    
    elif any([samp not in reporter_group_samples for samp in sample_names]):
        print("Some samples are not utilised in the provided selector, filtering out these samples...")
        sample_names = set(sample_names) & set(reporter_group_samples)


    selector_id = db[['reporter_group_id']].where(db['reporter_group_name'] == reporter_group_name).fetchone()
    sample_id = set(db[['sample_id']].where(db['sample_name'].in_(sample_names)).to_list())

    reporter_ids = set(db[['reporter_id']].where(db['reporter_group_id'] == selector_id).to_list())
    sample_reporters = set(db[['reporter_id']].where((db['sample_id'].in_(sample_id)) ).to_list())
    reporter_ids = reporter_ids & sample_reporters
    replicate_ids = set(db[['data_id']].where((db['reporter_group_id'] == selector_id) & (db['sample_id'].in_(sample_id))).to_list())

    print(replicate_ids)
    replicate_info = {}
    q = db.select(['data_id','sample_name','data_type', 'replicate_name']).where(db['data_id'].in_(replicate_ids)).to_dict(key = ['data_id'])
    for k,v in q.items():
        sname, dtype, replicate_name = v
        replicate_name = replicate_name.split('-')[-1]
        replicate_info[k] = f'{dtype} {sname} {replicate_name}'


    replicate_group = db.select(['data_group_id','data_id']).where(db['data_id'].in_(replicate_ids)).to_dict(group_by_key = True)
    
    #reporter_lookup, column_lookup, replicate_data = db.select(['reporter_id','data_id','processed_data_value']).where((db['reporter_id'].in_(reporter_ids)) & (db['data_id'].in_(list(replicate_ids)))).to_numpy(row_ids = reporter_ids, row_key = 'reporter_name', column_ids = replicate_ids)
    extracted_df = db.select(['reporter_id','data_id','processed_data_value']).where((db['reporter_id'].in_(reporter_ids)) & (db['data_id'].in_(list(replicate_ids)))).to_df()#row_ids = reporter_ids, row_key = 'reporter_name', column_ids = replicate_ids)
    
    corr_df = extracted_df.pivot(index='reporter_id',columns='data_id',values='processed_data_value').corr()
    corr_df.rename(columns = replicate_info, index = replicate_info,inplace=True)
    
    

    cluster_grid = sns.clustermap(corr_df,annot=True,fmt='.3f')

    cluster_grid.ax_heatmap.set_xlabel('')
    cluster_grid.ax_heatmap.set_ylabel('')
    cluster_grid.ax_heatmap.tick_params(axis='both', labelsize=12)

    out_path = out_path+f"/Replicate cluster_of_{reporter_group_name}_{'_'.join(sample_names)}_.{fig_save_format}"

    cluster_grid.figure.savefig(out_path, bbox_inches='tight')

    cluster_grid.figure.show()

 



