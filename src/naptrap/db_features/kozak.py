import json




def add_kozaks(db, kozak_path):

    kozak_id = ['kozak_id', 'frequency_matrix']

    if 'kozak' not in db.tables:
        db.new_table('kozak', kozak_id, [str, str], primary_key = 'kozak_id')

    kozak_dic = json.load(open(kozak_path, 'r'))

    for species_id, count_mat in kozak_dic.items():
        db.insert('kozak', kozak_id, [species_id, json.dumps(count_mat)], single=True)
    



def get_kozak_score(seq, frequency_matrix):

    kz_score = 0
    seq = (15 - len(seq)) * 'K' + seq

    for i,b in enumerate(seq):
        if b != 'K' and b != 'N':
            kz_score += frequency_matrix[b][i]
        else:
            kz_score += 25

    return kz_score



def score_kozak(db):

    main_kozak_score = []
    seq_5p, seq_3p, orf_idx = db.cursor.execute(f'SELECT adaptor_5p, adaptor_3p, main_orf_start FROM reporter_attr').fetchall()[0]
    seqlen_5p = len(seq_5p)
    seqs = db.cursor.execute(f'SELECT reporter_id, insert_sequence FROM reporters').fetchall()
    
    for rid, s in seqs:
        rseq = seq_5p + s + seq_3p
        orf_start = seqlen_5p + len(s) + orf_idx
        kozak_seq = rseq[orf_start - 9:orf_start + 6]
        for species, frequency_matrix in db.kozak_lookup.items():
            main_kozak_score.append([rid, species, kozak_seq ,get_kozak_score(kozak_seq, frequency_matrix)])



    db.new_table('main_kozak', ['reporter_id', 'species', 'kozak_seq', 'kozak_score'], [str, str, str, int])
    db.insert('main_kozak', ['reporter_id', 'species', 'kozak_seq', 'kozak_score'], main_kozak_score)


