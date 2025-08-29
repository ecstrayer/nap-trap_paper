import json
from mpradb.db_features import kozak


class ORF:

    def __init__(self, start, kozak_seq, frame, reporter_id, kozak_lookup):

        self.start = start
        self.start_codon = kozak_seq[9:12]
        self.kozak_seq = kozak_seq
        self.frame = frame
        self.frame_type = 'in_frame' if frame == 0 else 'out_of_frame'
        self.stop = start
        self.seq = str(self.start_codon)
        self.status = 1
        self.reporter_id = reporter_id
        self.orf_ids = ['reporter_id', 'type', 'start_codon', 'start', 'stop', 'frame', 'frame_type', 'kozak_seq', 'seq', 'length']
        self.add_kozak_score(kozak_lookup)

    def add_codon(self, codon):

        self.seq += codon
        self.stop += 3

    def orf_complete(self, stop_codon, orf_type):

        self.status = 0
        self.seq += stop_codon
        self.stop += 3
        self.type = orf_type
        self.length = len(self.seq)
        if orf_type == 'uorf':
            self.frame_type = 'all'



    def add_kozak_score(self, kozak_lookup):
    
        for kozak_id, frequency_mat in kozak_lookup.items():
            tmp_kozak = kozak.get_kozak_score(self.kozak_seq, frequency_mat)
            score_id = f'kozak_score_{kozak_id}'
            setattr(self, score_id, tmp_kozak)
            self.orf_ids.append(score_id)
    
    @property
    def orf_stats(self):
        return [getattr(self, x) for x in self.orf_ids]



def find_orfs(seq, reporter_id, main_orf_start, kozak_lookup, start_codons = ['ATG']):

    seqlen = len(seq)
    orfs = [[], [], []]


    for n in range(seqlen):
        frame = (main_orf_start - n) % 3
        frame = 3-frame if frame != 0 else frame
        codon = seq[n:n+3]


        orf_status = [sum(o.status for o in f) for f in orfs]
        
        if sum(orf_status) == 0 and n > main_orf_start - 3:
            break

        if orf_status[frame] > 0:
            for orf in orfs[frame]:
                if orf.status == 1:
                    if codon not in ['TAA', 'TAG', 'TGA']:
                        orf.add_codon(codon)
                
                    else:
                        orf_type = 'uorf' if n < main_orf_start else 'oorf'
                        orf.orf_complete(codon, orf_type)


        if codon in start_codons and n < main_orf_start:
            kozak_seq = seq[n-9:n+6]
            orfs[frame].append(ORF(n, kozak_seq, frame, reporter_id, kozak_lookup))

        
    return [o for frame in orfs for o in frame]


#reporters is a list of tuples of the form (reporter_id, seq)

def batch_orfs(reporters, adaptors, kozak_lookup):
    all_orfs = []
    upstream_seq, downstream_seq, main_orf_start = adaptors
    main_orf_start = int(main_orf_start)

    for reporter_id, seq in reporters:
        orfs = find_orfs(upstream_seq + seq + downstream_seq, reporter_id, main_orf_start = len(upstream_seq) + len(seq) + main_orf_start, kozak_lookup = kozak_lookup)
        all_orfs += orfs

    orf_ids = [f'orf_{o}' if o != 'reporter_id' else o for o in all_orfs[0].orf_ids]
    all_orfs = [orf.orf_stats for orf in all_orfs]

    return orf_ids, all_orfs

    
#need to add params functionality here

def add_uorfs(db, params):

    kozak_path = db.get_constants(['kozak_score_path'])[0]
    seq_attr = db.get_constants(['adaptor_5p','adaptor_3p','main_orf_start'])
    kozak_lookup = json.load(open(kozak_path,'r'))
    orf_ids, orfs = batch_orfs(db.reporters, seq_attr, kozak_lookup)
    orf_dtype =  [type(c) for c in  orfs[0]]

    db.new_table('uorf', orf_ids, orf_dtype, primary_key = 'reporter_id, orf_start', foreign_keys = [('reporter_id', 'reporter', 'reporter_id')])
    db.insert('uorf', orf_ids, orfs)



def to_db(db, params):

    if 'uorf' not in db.tables: 
        add_uorfs(db, params)


    #need to replace existing select statement

    #db.select(['reporter_id', 'orf_start_codon','orf_frame','orf_frame_type']).count().groupby([db['reporter_id'],db['orf_start_codon'], db['orf_type'], db['orf_frame_type']]) 

    #make column names better
    db.cursor.execute('''SELECT reporter_id, orf_start_codon, orf_type, orf_frame_type,  count(orf_type) as orf_number, AVG(orf_kozak_score_Homo_sapien) as orf_mean_kozak_score_hs, AVG(orf_kozak_score_Danio_rerio) as mean_kozak_score_dr, AVG(orf_length) as mean_orf_length FROM uorf
                     GROUP BY reporter_id, orf_start_codon, orf_type, orf_frame_type
                  ''')
    
    uorf_data = db.cursor.fetchall()
    uorf_ids = [x[0] for x in db.cursor.description]
    
    feature_names = []
    feature_data = []
    reporter_ids = []
    feature_types = []

    for s in uorf_data:
        rid = s[0]
        feature_prefix = '_'.join([s[1],s[2],s[3]])

        for fname, fd in zip(uorf_ids[4:], s[4:]):
            fname = '_'.join([feature_prefix, fname])
            feature_names.append(fname)
            feature_data.append(fd)
            reporter_ids.append(rid)
            ftype = 'uorf_stats' if 'kozak' not in fname else f'uorf_stats_{fname.split("_")[-1]}'
            feature_types.append(ftype)


            
    db.add_features(feature_names = feature_names, reporter_ids = reporter_ids, feature_data = feature_data, feature_types = feature_types)
