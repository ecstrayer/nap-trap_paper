import numpy as np

def to_db(db, params):

    kmax = params.get('kmax')
    pos_to_exclude = params.get('pos_to_exclude')
    pos_adjust = params.get('pos_adjust')
    pos_0 = params.get('pos_0')

    kmer_positions = db['kmer','position','reporter_id'].where(db['kmer'].length() <= kmax).to_list()
    feature_names = []
    reporter_ids = []
    feature_types = []
    data = np.ones(len(kmer_positions))


    for kmer, pos, rid in kmer_positions:
        if pos not in pos_to_exclude:
            if pos_adjust is not None:
                pos = pos + pos_adjust
                if not pos_0 and pos >= 0:
                    pos = pos + 1

            feature_names.append(f'{kmer}_{pos}')
            reporter_ids.append(rid)
            feature_types.append(f'hot_encoded_{len(kmer)}')

    
    db.add_features(feature_names = feature_names, reporter_ids = reporter_ids, feature_data = data, feature_types = feature_types)