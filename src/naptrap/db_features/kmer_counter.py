import itertools

class Unique_counter:

    def __init__(self, kmax, reporter_id):

        self.kmer_ids = ['reporter_id','position', 'kmer']
        self.kmer_queue = [[] for n in range(kmax)]
        self.kmax = kmax
        self.reporter_id = reporter_id
        self.all_kmers = []
        

    def add_kmers(self, kmer_list):

        self.kmer_queue = self.kmer_queue[1:] + [[]]
        all_kmers = self.stored_kmers

        kmer_list = [k for k in kmer_list if not k in all_kmers]

        for k in kmer_list:
            self.kmer_queue[len(k) - 1].append(k)
        
        self.all_kmers.append(kmer_list)
        

    @property
    def stored_kmers(self):
        return set([k for i in self.kmer_queue for k in i])

    @property
    def get_kmer_counts(self):
        kmer_counts = []
        for i, kl in enumerate(self.all_kmers):
            for k in kl:
                kmer_counts.append(tuple([self.reporter_id, i, k]))

        return kmer_counts
    


#returns a list of tuples of the form (reporter_id, kmer, list)

def batch_kmers(reporters, kmax = 8):

    kmer_data = []

    for reporter_id, seq in reporters:
        seqlen = len(seq)
        ucounter = Unique_counter(kmax, reporter_id)

        for i in range(seqlen):
            km = seqlen - i if i + kmax > seqlen else kmax
            kmers = [seq[i:i+x] for x in range(1, km + 1)]
            ucounter.add_kmers(kmers)

        kmer_data += ucounter.get_kmer_counts

    kmer_ids = ucounter.kmer_ids
    return kmer_ids, kmer_data

def add_kmers(db, kmax = 6):

    kmer_ids, kmers = batch_kmers(db.reporters, kmax)
    kmer_dtype = list(map(type, kmers[0]))
    db.new_table('kmer_position_', kmer_ids, kmer_dtype, foreign_keys = [('reporter_id', 'reporter', 'reporter_id')])
    db.insert('kmer_position_', kmer_ids, kmers)


#we could cluster features into reporter string, but this is a bit more flexible

def to_db(db, params):

    kmax = params.get('kmax')
    kmin = params.get('kmin')     

    add_kmers(db, kmax)
    
    kmer_counts = db.select(['reporter_id', 'kmer']).groupby(['reporter_id','kmer'], aggregate_functions={'count' :['kmer']}).fetchall()
    
    feature_names = []
    reporter_ids = []
    feature_data = []
    feature_types = []

    for rid,kmer,count in kmer_counts:
        feature_names.append(f'{kmer}_count')
        reporter_ids.append(rid)
        feature_data.append(count)
        feature_types.append(f'count_{len(kmer)}mer')
        
    db.add_features(feature_names = feature_names, reporter_ids = reporter_ids, feature_data = feature_data, feature_types = feature_types)

