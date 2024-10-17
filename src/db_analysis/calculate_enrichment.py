import scipy.stats

def calculate_enrichment(fcount, parent_ids, group_rids):

    pval_list = []
    total_counts = fcount[parent_ids,:].sum()
    total_group_counts = fcount[group_rids, :].sum()
    group_fcount = fcount[group_rids, :].sum(axis = 0)
    all_fcount = fcount[parent_ids,:].sum(axis = 0)

    for gc,ac in zip(group_fcount, all_fcount): 
        pval = scipy.stats.hypergeom.sf(gc - 1, total_counts , ac, total_group_counts) if ac != 0 else -1
        fold_change = (gc / total_group_counts) / (ac / total_counts) if ac != 0 else -1

        pval_list.append([pval, fold_change])

    return pval_list


def get_feature_enrichment(db, reporter_group_ids_added, feature_type):

    feature_enrich_ids = ['reporter_group_id', 'feature_id', 'enrichment_pvalue','enrichment_fold_change']
    feature_enrich_foriegn_keys = [('reporter_group_id','reporter_group_attribute','reporter_group_id'), ('feature_id','feature_attribute','feature_id')]
    feature_enrich_dtype = [int, int, float, float]

    if 'feature_enrichment_iso' not in db.tables:
        db.new_table('feature_enrichment_iso', feature_enrich_ids, feature_enrich_dtype, primary_key = 'reporter_group_id,feature_id', foreign_keys = feature_enrich_foriegn_keys)
    
    feature_ids = db.select(['feature_id']).where(db['feature_type'].like(f'%{feature_type}%')).to_list()
    _, feature_id_lookup, fcount = db.select(['reporter_id','feature_id','feature_value']).where(db['feature_type'].like(f'%{feature_type}%')).to_numpy(column_ids = feature_ids)
    feature_id_lookup = {v:k for k,v in feature_id_lookup.items()}

    fenrich = []

    for rgid in reporter_group_ids_added:
        parent_id = db.select(['reporter_group_parent_id']).where(db['reporter_group_id'] == rgid).fetchone()
        gids = db.select(['reporter_id']).where(db['reporter_group_id'] == rgid).to_list()
        pids = db.select(['reporter_id']).where(db['reporter_group_id'] == parent_id).to_list()
       
        pval_list = calculate_enrichment(fcount = fcount, parent_ids = pids, group_rids = gids)
        for n,p in enumerate(pval_list):
            fenrich.append([rgid, feature_id_lookup[n], *p])
    c  = [tuple(f[:2]) for f in fenrich]
    
    db.insert(table_name = 'feature_enrichment_iso', columns = feature_enrich_ids, values = fenrich)


def get_kmer_enrichment(db,reporter_group_ids_added, kmin, kmax):

    for n in range(kmin,kmax + 1):
        get_feature_enrichment(db = db, reporter_group_ids_added= reporter_group_ids_added, feature_type= f'count_{n}mer')