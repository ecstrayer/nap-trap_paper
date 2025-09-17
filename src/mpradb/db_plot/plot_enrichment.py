import numpy as np
import matplotlib.pyplot as plt; plt.ion()
import os

import mpradb.db_plot.plotter as plotter



def plot_enrichment(db, selector_id, klen = 6):

    pthresh = 0.05 / 4 ** klen
    glabel_list = ['repressed', 'active', 's1_activated', 's2_activated']
    pal = ["#E69F00", "#56B4E9", "#009E73", "#CC79A7", "#999999"]

    rgroup_lookup = db['reporter_group_id','reporter_group_name','reporter_group_type'].where(db['reporter_group_parent_id'] == selector_id).to_dict(key = ['reporter_group_id'])
    rgids = list(rgroup_lookup.keys())
    kmers = db['reporter_group_id', 'feature_name','enrichment_pvalue','enrichment_fold_change'].where((db['reporter_group_id'].in_(rgids)) & (db['feature_type'] == f'count_{klen}mer') &  (db['enrichment_pvalue'] != 1)).to_dict(group_by_key = True, key = ['reporter_group_id'])


    for rgid, fenrich in kmers.items():
        fenrich = sorted(fenrich, key = lambda a : a[-1])
        group_name, group_type = rgroup_lookup[rgid]
        cidx = glabel_list.index(group_type)
    
        pval_list,fc_list, fn_labels = [],[],[]
        
        for fname, pval, fc in fenrich:
            pval = 1-pval if pval >= 0.5 else pval
            pval_list.append(-np.log10(pval))
            fc_list.append(np.log2(fc))
            fn_labels.append(fname.split('_')[0].replace('T','U'))

        fig,ax = plotter.plot_scatter(x = pval_list, y = fc_list, c = pal[cidx], xlabel = '-log10(pvalue)', ylabel = 'log2(fold change)', set_maxv = False, show_pearsonr = False, plot_density= False, rasterized= False)
        ax.axvline(-np.log10(pthresh), c = '#989798', linewidth = 0.24, linestyle = '-')

        i = 0
        for p,f,s in zip(pval_list, fc_list, fn_labels):
            ax.text(x = p, y = f, s = s, fontsize = 6, color = pal[cidx])
            i += 1
            if i == 10:
                break


    



