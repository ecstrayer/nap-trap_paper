import subprocess 
import multiprocessing
import os 

import numpy as np




class Rfold:

    struct_intervals = [20,50,100,200]

    def __init__(self,reporter_id):
        self.reporter_id = reporter_id
        self.pp_feature_names = []

    def add_info(self, i, l):

        x = l.split(' ')
  
        if i == 0:
            self.sequence = x[0]
        elif i == 1:
            self.mfe_structure = x[0]
            self.mfe = float(x[-1].strip('()'))
        elif i == 2:
            self.pairing_prob = x[0]
            self.mea = float(x[-1].strip('[]'))
        elif i == 3:
            self.centriod_structure = x[0]
            self.centriod_mfe = float(x[-2].strip('{ '))
            self.centriod_distance = float(x[-1].strip('d=} '))
        elif i == 4:
            x = l.replace('diversity','').replace(';','').replace(' ','')
            x = x.split('ensemble')
            self.frequency_of_mfe = float(x[1])
            self.ensemble_diversity = float(x[2])
        else:
            raise Exception(f'Invalid index: {i,l}')

    def add_pairing_probability(self, pp):

        seq_len = len(pp)
        for i in self.struct_intervals:
            for p in range(0,seq_len, i):
                fname = f'pairp_int-{i}_pos-{p}'
                self.pp_feature_names.append(fname)
                setattr(self, fname, np.mean(pp[p:p+i]))

    def add_orfs(self,orf_list):
        pass
        



    def to_db(self,feature_names):
        x = []
        for f in feature_names:
            x.append(getattr(self, f))
        
        return x

        






def read_dp(fpath, seq_len = 200):

    rid = fpath.split('/')[-1].strip('_dp.ps')


    bp = False

    dp_mfe = np.zeros([seq_len, seq_len])
    dp_all = np.zeros([seq_len, seq_len])

    with open(fpath,'r') as f:
        for l in f:
            if bp:
                try:
                    l = l.strip('\n')
                    x,y,p,t = l.split()  
                    x = int(x)- 1
                    y = int(y) -1
                    if t == 'lbox':
                        dp_mfe[x,y] = float(p) ** 2
                    elif t == 'ubox':
                        dp_all[x,y] = float(p) ** 2
                except:
                    break
            elif 'start of base pair probability data' in l:
                bp = True
            else:
                continue
    
    os.remove(fpath)
    return tuple([rid, dp_all.sum(axis = 0) + dp_all.sum(axis = 1)])




def calculate_structure(db, rna_temp = 28, outpath = None):


    rnafold_output = {}

    adaptor_5p, adaptor_3p, cds_start = db.get_constants(['adaptor_5p','adaptor_3p','main_orf_start'])
    cds_start = int(cds_start)
    adaptor_3p = adaptor_3p[:cds_start + 37]

    fasta_path = f'{outpath}tmp.fa'
    dot_path = []

    with open(fasta_path,'w') as f:
        for rname, reporter in db.select(['reporter_name','insert_sequence']).fetchall():
            utr5_seq = adaptor_5p + reporter + adaptor_3p
            f.write(f'>{rname}\n{utr5_seq}\n')
            dot_path.append(f'{outpath}{rname}_dp.ps')

    proc = subprocess.Popen(['RNAfold','-j20', '-p','-T', str(rna_temp), 'tmp.fa'], stdout = subprocess.PIPE,cwd= outpath)
    sout = proc.stdout

    for l in sout:
        l = l.decode('utf-8').strip('\n')
        if l.startswith('>'):
            rid = l.strip('>')
            rnafold_output[rid] = Rfold(rid)
            i = 0
        else:
            rnafold_output[rid].add_info(i,l) 
            i += 1
    


    with multiprocessing.Pool(20) as p:
        r = p.map(read_dp,dot_path)
        
    for rid, pp in r:
        rnafold_output[rid].add_pairing_probability(pp)

    return rnafold_output


def add_uaugs(db, rnafold_output):

    if 'uorf' not in db.tables:
        raise Exception('Please add uorfs to db; using orf_finder')

    uorf_dic = db.select(['reporter_name','orf_type','orf_start_codon','orf_start','orf_stop']).to_dict(key='reporter_name', group_by_key=True)

    for rname in uorf_dic.items():
        rnafold_output[rname].add_orfs(uorf_dic)
        


def to_db(db, params):

    rna_temperature = params['rna_temperature']
    outpath = params['outpath']
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    rnafold_output = calculate_structure(db, rna_temp = rna_temperature, outpath= outpath)

    reporter_ids = []
    feature_data = []
    feature_types = []

    ftype = f'rnafold_{rna_temperature}C'

    feature_names = []
    rname_lookup = db.select(['reporter_name','reporter_id']).to_dict()

    for i,(rname, r) in enumerate(rnafold_output.items()):
        if i == 0:
            feature_names = []
            fnames = ['mfe', 'mea', 'frequency_of_mfe','ensemble_diversity'] + r.pp_feature_names
            fname_to_add = [f'{f}-{rna_temperature}' for f in fnames]
            feature_num = len(fnames)
        rid = rname_lookup[rname]
        feature_data += r.to_db(fnames)
        feature_names += fname_to_add

        for n in range(feature_num):
            reporter_ids.append(rid)
            feature_types.append(ftype)

    db.add_features(feature_names = feature_names, reporter_ids = reporter_ids, feature_data = feature_data, feature_types = feature_types)
        





    