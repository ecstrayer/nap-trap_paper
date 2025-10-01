from random import sample
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt; #plt.ion()
import os
import seaborn as sns
import mpradb.db_plot.plotter as plotter



def plot_enrichment(db, reporter_group_name,fig_save_path,sample_names=None, klen = 6,fig_save_format = None):

    if fig_save_format == None:
        fig_save_format = 'svg'

    reporter_group_id = db[['reporter_group_id']].where(db['reporter_group_name'] == reporter_group_name).fetchone()

    reporter_group_parent_id = reporter_group_id 

    data_groups = set(db[['data_group_id']].where(db['reporter_group_id'] == reporter_group_id).to_list())
    sample_data_groups = set(db['sample_name'].where(db['data_group_id'].in_(data_groups)).to_list())

    if sample_names == None:

        sample_names = list(set(db['sample_name'].where(db['data_group_id'].in_(data_groups) ).to_list()))

    fig_save_path = fig_save_path+f'/figures/{reporter_group_name}/'


    if not os.path.exists(fig_save_path):
        os.makedirs(fig_save_path)

    hist_flag,scatter_flag = False,False
    if type(sample_names) in {list,set,tuple}:
        if len(sample_names) > 1 and len(sample_names) < 3 and set(sample_names) <= sample_data_groups:
            
            #sample_names = list(sample_names)
            #sample_names = sorted(sample_names)

            df1 = db[['reporter_id','reporter_group_name','reporter_group_parent_id','reporter_group_type']].where(db['reporter_group_parent_id']==reporter_group_parent_id).to_df()
            df2 = db[['reporter_id','sample_name','processed_data_value']].where(db['sample_name'].in_(list(sample_names))).to_df()

            df = df1.merge(df2,on='reporter_id')

            tcompares = df['reporter_group_name'].apply(lambda x: ('s1' in x)and (all([samp in x for samp in sample_names])) and ('s2' in x)).tolist()
            
            #tcompares = df['reporter_group_name'].apply(lambda x: ('s1' in x) and ('s2' in x)).tolist()



            #sns.scatterplot(df, )
            if any(tcompares)  :
                df = df[tcompares]


            df = df.groupby(['reporter_id','reporter_group_name','reporter_group_type','sample_name'],as_index=False)['processed_data_value'].mean()

            reporter_list = db['reporter_id'].where(db['reporter_group_name']==reporter_group_name).to_list()

            reporter_rel_list = df['reporter_group_name'].unique()

            for rep in reporter_rel_list:
                if 's1' in rep and 's2' in rep:
                    trep = [r1.split('_') for r1 in rep.split('-') ]
                    s1 = '_'.join(trep[0][1:]) + ' activated'
                    s2 = '_'.join(trep[2][1:]) + ' activated'
                    print(s1,s2)
                    break


            df = df.pivot_table( index= ['reporter_id','reporter_group_type'],columns = 'sample_name',values='processed_data_value').reset_index()
            fulldf = db[['reporter_id','sample_name','processed_data_value']].where((db['reporter_id'].in_(reporter_list)) & (db['sample_name'].in_(sample_names))).to_df()
            fulldf = fulldf.groupby(['reporter_id','sample_name'],as_index=False)['processed_data_value'].mean()
            fulldf = fulldf.pivot_table(index='reporter_id',columns = 'sample_name',values= 'processed_data_value').reset_index()


            pal = {'repressed':"#E69F00",'active': "#56B4E9",'s2_activated': "#009E73",'s1_activated': "#CC79A7",'selector': "#999999"}
            fig,ax = plotter.plot_scatter(x = fulldf[sample_names[0]],y = fulldf[sample_names[1]],xlabel=sample_names[0],show_pearsonr=False,ylabel=sample_names[1],c = 'gray',plot_density=False)
            fig_ax = [fig,ax]
            df.sort_values(by='reporter_group_type')
            for it,rgt in enumerate(np.sort(df['reporter_group_type'].unique())[::-1]):
                x = df.loc[df['reporter_group_type'] == rgt,sample_names[0]].tolist()

                y = df.loc[df['reporter_group_type'] == rgt,sample_names[1]].tolist()

                label = s1 if 's1' in rgt else s2 if 's2' in rgt else rgt

                fig,ax = plotter.plot_scatter(x = x,y = y,xlabel=sample_names[0],ylabel=sample_names[1],c = pal[rgt],show_pearsonr=False,plot_density=False,fig_ax=fig_ax,scatter_label=label)

                fig_ax = [fig,ax]
            plt.legend(fontsize=2)
            ax.set_xlim(0, np.max(fulldf[sample_names[0]])+1)
            ax.set_ylim(0, np.max(fulldf[sample_names[1]])+1)

            fig.savefig(f"{fig_save_path}{reporter_group_name}_{sample_names[0]}_vs_{sample_names[1]}_scatter_plot.{fig_save_format}")
            fig.show()
            scatter_flag = True

        elif len(sample_names) == 1:
            hist_flag = True
            sample_names = sample_names[0]
        elif len(data_groups) > 2:
            print("Can only plot scatter plot or histogram of <=2 sample sets ... Plotting enrichment only")
        else:
            print("Can only plot scatter plot or histogram of translation values ... Plotting enrichment only")

    if hist_flag or isinstance(sample_names,str):

        data_group_ids = db[['data_group_id']].where(db['sample_name'] == sample_names).fetchone()
        selector_ids = db['reporter_group_id','reporter_group_name'].where((db['data_group_id'] == data_group_ids) & (db['reporter_group_parent_id'] == reporter_group_parent_id)).to_df()
        tcompares = selector_ids['reporter_group_name'].apply(lambda x: ('s1' in x) and ('s2' in x)).tolist()

        if any(tcompares)  :
            selector_ids = selector_ids[[not tcomp for tcomp in tcompares]]

        selector_ids = selector_ids['reporter_group_id'].tolist()
        #rnum_calc  = len(db['reporter_id','reporter_group_type'].where((db['reporter_group_id'].in_(selector_ids))).to_list())/2

        rnum_calc = db[['reporter_group_description','reporter_group_id']].where(db['reporter_group_id'].in_(selector_ids)).fetchone()

        rnum_calc = float(rnum_calc.split(' ')[-1])
        print(rnum_calc)

        reporter_list = db['reporter_id'].where(db['reporter_group_name']==reporter_group_name).to_list()
        fulldf = db[['reporter_id','sample_name','processed_data_value']].where((db['reporter_id'].in_(reporter_list)) & (db['sample_name'] == (sample_names))).to_df()
        fulldf = fulldf.groupby(['reporter_id','sample_name'],as_index=False)['processed_data_value'].mean()

        #rnum_calc = rnum_calc/num_rep

        #fulldf = fulldf.pivot_table(index='reporter_id',columns = 'sample_name',values= 'processed_data_value').reset_index()
        top_p = fulldf.processed_data_value.quantile(1-rnum_calc)
        bottom_p = fulldf.processed_data_value.quantile(rnum_calc)
        pal = {'Activated': "#56B4E9",'Repressed':"#E69F00",'No Change': "#999999"}
        fulldf[''] = fulldf['processed_data_value'].apply(lambda x: 'Activated' if x > top_p else 'Repressed' if x < bottom_p else 'No Change')
        fig,ax = plt.subplots(1,1)
        ax = sns.histplot(fulldf,x='processed_data_value',hue = '',multiple='stack',bins=100,palette=pal,ax=ax)
        plt.xlabel(f"{sample_names} Translation")
        plt.ylabel(f"Reporter Count")
        fig.savefig(f"{fig_save_path}{reporter_group_name}_{sample_names}_histogram_plot.{fig_save_format}")
        plt.show()
        hist_flag = True

    if not(hist_flag or scatter_flag) :
        print("sample_names not in the supported format , please provide a string, list, tuple or pandas series")


    pthresh = 0.05 / 4 ** klen
    glabel_list = ['repressed', 'active', 's1_activated', 's2_activated']
    pal = ["#E69F00", "#56B4E9", "#009E73", "#CC79A7", "#999999"]

    rgroup_lookup = db['reporter_group_id','reporter_group_name','reporter_group_type'].where(db['reporter_group_parent_id'] == reporter_group_parent_id).to_dict(key = ['reporter_group_id'])
    rgids = list(rgroup_lookup.keys())
    kmers = db['reporter_group_id', 'feature_name','enrichment_pvalue','enrichment_fold_change'].where((db['reporter_group_id'].in_(rgids)) & (db['feature_type'] == f'count_{klen}mer') &  (db['enrichment_pvalue'] != 1)).to_dict(group_by_key = True, key = ['reporter_group_id'])

    

    for rgid, fenrich in kmers.items():

        if hist_flag:
            if rgid not in selector_ids:
                continue
            rpgn = db['reporter_group_name'].where(db['reporter_group_id']==rgid).fetchone()

            if rpgn.split('-')[0] not in sample_names:
                continue
        elif scatter_flag == True:
            scatter_group_ids = db['reporter_group_id'].where(db['reporter_group_name'].in_(list(reporter_rel_list))).to_list()
            if rgid not in scatter_group_ids:
                continue

        fenrich = sorted(fenrich, key = lambda a : a[-1])
        group_name, group_type = rgroup_lookup[rgid]
        cidx = glabel_list.index(group_type)
    
        pval_list,fc_list, fn_labels = [],[],[]
        
        for fname, pval, fc in fenrich:
            pval = 1-pval if pval >= 0.5 else pval
            pval_list.append(-np.log10(pval))
            fc_list.append(np.log2(fc))
            fn_labels.append(fname.split('_')[0].replace('T','U'))

        fig,ax = plotter.plot_scatter(x = pval_list, y = fc_list, c = pal[cidx], xlabel = '-log10(pvalue)', ylabel = 'log2(fold change)', set_maxv = False, show_pearsonr = False, plot_density= False, rasterized= True)
        ax.axvline(-np.log10(pthresh), c = '#989798', linewidth = 0.24, linestyle = '-')

        i = 0
        for p,f,s in zip(pval_list, fc_list, fn_labels):
            sorted_f = sorted(fc_list)
            if p > -np.log10(pthresh) and (f in sorted_f[:10] or f in sorted_f[len(sorted_f)-10:]):
                ax.text(x = p, y = f, s = s, fontsize = 2, color = pal[cidx])
            i += 1
            #if i == 10:
            #    break
        fig.savefig(f"{fig_save_path}Reporter_group_{rgroup_lookup[rgid][0]}_RGID_{rgid}_type_{rgroup_lookup[rgid][1]}.{fig_save_format}")
    return_df =  db['reporter_group_name', 'feature_name','enrichment_pvalue','enrichment_fold_change'].where((db['reporter_group_id'].in_(rgids)) & (db['feature_type'] == f'count_{klen}mer') &  (db['enrichment_pvalue'] != 1)).to_df()

    return_df['feature_name'] = return_df['feature_name'].apply(lambda x: x.split('_')[0])

    return return_df
    



