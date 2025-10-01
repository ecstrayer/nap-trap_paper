
from random import sample
import sys 
import itertools
import numpy as np
import datetime
import os
from mpradb.database.mpra_db import MPRA_DB
import mpradb.db_plot.plotter as plotter 


def plot_replicates(db, selector_name,output_path,sample_names = None, reporter_names_to_label = None,fig_save_format = None, reporter_labels = None, colors = None):


    out_path = f'{output_path}/figures/replicate_comparision/{selector_name}/'

    if not os.path.exists(out_path):
        os.makedirs(out_path)

    if fig_save_format == None:
        fig_save_format = 'svg'

    selector_id = db[['reporter_group_id']].where(db['reporter_group_name'] == selector_name).fetchone()


    reporter_group_id = db[['reporter_group_id']].where(db['reporter_group_name'] == selector_name).fetchone()
    data_groups = set(db[['data_group_id']].where(db['reporter_group_id'] == reporter_group_id).to_list())

    reporter_group_samples  = list(set(db['sample_name'].where(db['data_group_id'].in_(data_groups) ).to_list()))


    if sample_names == None:

        sample_names = reporter_group_samples
    if isinstance(sample_names,str):
        sample_names = [sample_names]


    sample_id = set(db[['sample_id']].where(db['sample_name'].in_( sample_names)).to_list())
    reporter_ids = set(db[['reporter_id']].where(db['reporter_group_id'] == selector_id).to_list())
    sample_reporters = set(db[['reporter_id']].where((db['sample_id'].in_(sample_id)) ).to_list())
    reporter_ids = reporter_ids & sample_reporters
    replicate_ids = set(db[['data_id']].where((db['reporter_group_id'] == selector_id)).to_list())
    replicate_ids = set(db[['data_id']].where((db['sample_id'].in_(sample_id)) & (db['data_id'].in_(replicate_ids))).to_list())

    replicate_info = {}
    q = db.select(['data_id','sample_name','data_type', 'replicate_name']).where(db['data_id'].in_(replicate_ids)).to_dict(key = ['data_id'])
    for k,v in q.items():
        sname, dtype, replicate_name = v
        replicate_name = replicate_name.split('-')[-1]
        replicate_info[k] = f'{dtype} {sname} {replicate_name}'


    replicate_group = db.select(['data_group_id','data_id']).where(db['data_id'].in_(replicate_ids)).to_dict( group_by_key = True)
    reporter_lookup, column_lookup, replicate_data = db.select(['reporter_id','data_id','processed_data_value']).where((db['reporter_id'].in_(reporter_ids)) & (db['data_id'].in_(list(replicate_ids)))).to_numpy(row_ids = reporter_ids, row_key = 'reporter_name', column_ids = replicate_ids)


    h = db.get_hash('-'.join(reporter_names_to_label))[:5] if reporter_names_to_label is not None else ''

    for p in replicate_group.values():
        for r1,r2 in itertools.combinations(p, 2):
            xidx = column_lookup[r1]
            yidx = column_lookup[r2]
            xlabel = replicate_info[r1]
            ylabel = replicate_info[r2]


            if reporter_names_to_label is None:
                fig,ax = plotter.plot_scatter(x = replicate_data[:,xidx], y = replicate_data[:,yidx], xlabel=xlabel, ylabel=ylabel)
            else:
                mask = np.zeros(replicate_data.shape[0])
                rids = [reporter_lookup[r] for r in reporter_names_to_label]
                mask[rids] = 1
                r = (mask == 1)

                fig,ax = plotter.plot_scatter(x = replicate_data[~r,xidx], y = replicate_data[~r,yidx], xlabel=xlabel, ylabel=ylabel, c = 'gray', alpha = 0.5, plot_density= False, rasterized = False)
                fig,ax = plotter.plot_scatter(x = replicate_data[r,xidx], y = replicate_data[r,yidx], xlabel=xlabel, ylabel=ylabel, c = 'blue', s = 4, fig_ax = (fig,ax), plot_density = False, show_pearsonr = False, rasterized = False)

                for i,n in enumerate(rids):
                    t = reporter_names_to_label[i] if reporter_labels is None else reporter_labels[i]
                    ax.annotate(text = t, xy = (replicate_data[n,xidx], replicate_data[n,yidx]), fontsize = 6)
                
    
            xlabel = '-'.join(xlabel.split())
            ylabel = '-'.join(ylabel.split())    
            fname = f'X{xlabel}Y{ylabel}R{h}-'
            figdate = datetime.datetime.today().strftime('%m%d%y')
            fig.savefig(f'{out_path}{fname}{figdate}.{fig_save_format}')
                    

    return replicate_data[r,:] if reporter_names_to_label else replicate_data



