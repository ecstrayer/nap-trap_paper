def to_db(db):

    q = db.execute('SELECT reporter_id, feature_name, value from features join feature_attr using (feature_id) where feature_type == "kmer_count_1"')

    reporter = {}

    for rid, fname, val in q:
        rid = [int(r) for r in rid.split(',')]
        val = [int(v) for v in val.split(',')]
        for i,r in enumerate(rid):
            if r not in reporter:
                reporter[r] = {}
            reporter[r][fname] = val[i]

    feature_names = []
    feature_data = []
    reporter_ids = []

    dinucleotides = list(itertools.combinations(['A','T','G','C'], 2))
    for d in dinucleotides:
        for rid, data in reporter.items():
            reporter_ids.append(rid)
            feature_data.append(data[d[0]] + data[d[1]])
            feature_names.append(f'{d[0]}{d[1]}_richness')

    db.insert_features(feature_names, reporter_ids, feature_data, 'dinucleotide', add_length = False)


