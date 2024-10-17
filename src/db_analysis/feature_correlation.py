import numpy as np
import scipy.stats



def get_correlation(selector_id, features, feature_ids, data, data_group_ids):

    feature_correlation = []

    for n,fid in enumerate(feature_ids):
         for i,dgid in enumerate(data_group_ids):
            corr, pval = scipy.stats.spearmanr(features[:,n],data[:,i])
            corr = 0 if np.isnan(corr) else corr
            pval = 1 if np.isnan(pval) else pval
            feature_correlation.append([fid, selector_id, dgid, corr, pval])

    return feature_correlation




def to_db(db, params = {}):

    table_columns = ['feature_id', 'reporter_group_id', 'data_group_id', 'correlation', 'pvalue']
    table_data_types = [int, int, int, float, float]
    feature_correlation = []
    
    if 'feature_correlation_iso' not in db.tables:
        db.new_table('feature_correlation_iso', table_columns, table_data_types, primary_key = 'feature_id, reporter_group_id, data_group_id', foreign_keys =[('feature_id', 'feature', 'feature_id'), ('reporter_group_id','reporter_group_attribute', 'reporter_group_id'), ('data_group_id','data_group_attribute_iso', 'data_group_id')])

    
    for selector_name in db.selector_names:
        s = getattr(db,selector_name)
        feature_correlation += get_correlation(s.selector_id, s.features, list(db.feature_names.keys()), s.data, s.data_group_ids)
        
    db.insert('feature_correlation_iso', table_columns, feature_correlation)


    
