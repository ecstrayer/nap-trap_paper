import numpy as np
import scipy.stats
import itertools

from mpradb.db_analysis import calculate_enrichment



def make_rank_enrichment_groups(data, rnum):

    if type(rnum) == float:
        rnum = int(data.shape[0] * rnum)

    if data.ndim == 1:
        rank = scipy.stats.rankdata(data)
        rank_sort = np.argsort(rank)
        glabels = ['repressed', 'active']
        gidx = (rank_sort[:rnum], rank_sort[-rnum:])

    elif data.ndim == 2 and data.shape[-1] == 2:
        rank_d1 = scipy.stats.rankdata(data[:,0])
        rank_d2 = scipy.stats.rankdata(data[:,1])
        rank_sum = rank_d1 + rank_d2
        rank_diff = rank_d1 - rank_d2
        idx_rsum = np.argsort(rank_sum)
        idx_rdiff = np.argsort(rank_diff)
        glabels = ['repressed', 'active', 's1_activated', 's2_activated']
        gidx = (idx_rsum[:rnum], idx_rsum[-rnum:], idx_rdiff[-rnum:], idx_rdiff[:rnum])

    return gidx, glabels



def to_db(db, params):
    
    rnum = params.get('rnum')
    feature_types = params.get('feature_types')
    kmin = params.get('kmin')
    kmax = params.get('kmax')
    reporter_group_ids_added = []

    for s in db.selectors:
        for i, group_name in enumerate(s.data_group_lookup):
            group_idx,group_labels = make_rank_enrichment_groups(s.data[:,i], rnum)

            for gidx, gtype in zip(group_idx, group_labels):
                reporter_ids = [s.reporter_ids[int(i)] for i in gidx]
                group_description = f'selector: {s.selector_id}, reporter number: {rnum}'

                rgid = db.get_selector(reporter_ids = reporter_ids, reporter_group_type = gtype, data_group_ids = [s.data_group_lookup[group_name]], description = group_description, reporter_group_name = group_name, reporter_group_parent_id = s.selector_id)
                reporter_group_ids_added.append(rgid)

        data_group_names = list(s.data_group_lookup.keys())
        dgnum = len(data_group_names)

        for idx1, idx2 in itertools.combinations(range(dgnum),2):
            rg1 = data_group_names[idx1]
            rg2 = data_group_names[idx2]
            dgids = [s.data_group_lookup[g] for g in data_group_names]
            group_idx, group_labels = make_rank_enrichment_groups(s.data[:,[idx1,idx2]], rnum)

            
            for gidx, gtype in zip(group_idx, group_labels):
                reporter_ids = [s.reporter_ids[int(i)] for i in gidx]
                group_name = f's1_{rg1}-s2_{rg2}'
                group_description = f'selector: {s.selector_id}, reporter number: {rnum}'
                rgid = db.get_selector(reporter_ids = reporter_ids, reporter_group_type = gtype, data_group_ids = dgids, description = group_description, reporter_group_name = group_name, reporter_group_parent_id = s.selector_id)
                reporter_group_ids_added.append(rgid)
                
    calculate_enrichment.get_kmer_enrichment(db,reporter_group_ids_added, kmin = kmin, kmax = kmax)







