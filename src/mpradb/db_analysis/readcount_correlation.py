import numpy as np
import scipy.stats
import itertools

import src.db_plot.plotter as plotter



class Sample:

    def __init__(self, sample_id: str, reporter_num: int, replicate_num: int, increment: float = 0.05):
        self.sample_id = sample_id
        self.replicate_num = replicate_num
        self.values = np.zeros([reporter_num, replicate_num])
        self.min_reads = np.zeros(reporter_num)
        self.increment = increment

    def add_data(self, reporter_id: int, value: str, min_reads: int):

        add_val = [float(v) for v in value.split(',')]
        if len(add_val) == self.replicate_num:
            self.values[reporter_id, :] = add_val
            self.min_reads[reporter_id] = min_reads

    def data_process(self):
        non_zero_selector = np.where(self.min_reads > 0)
        self.values = self.values[non_zero_selector]
        self.min_reads = self.min_reads[non_zero_selector]

        self.quantiles = np.quantile(self.min_reads, np.arange(0, 1, self.increment))

    def calculate_correlation(self):

        self.output_ids = ['sample_id', 'quantile', 'read_filter', 'correlation', 'correlation_str', 'total_reporters']
        self.output_dtypes = [str, float, int, float, str, int]

        correlations = []

        q = 0
        
        for read in self.quantiles:
            read = int(read)
            selector = self.min_reads > read
            corr = np.corrcoef(self.values[selector,:].T)
            corr_str = ','.join([str(c) for c in corr])
            corr = corr[0] if len(corr) == 1 else float(np.min(corr))
            total_reporters = int(np.sum(selector))

            correlations.append([self.sample_id, read, q, corr, corr_str, total_reporters])
            q += self.increment     

        return correlations



def read_correlations(db, sample_ids):

    reporter_num = db.reporter_num
    slist = "'"+"','".join(list(sample_ids.keys()))+ "'"
    samples = db.cursor.execute(f'SELECT sample_id, reporter_id, group_concat(translation), min(input) FROM data where sample_id in ({slist}) group by  sample_id, reporter_id').fetchall()
    sample_num = len(samples)

    sample_dic = {s : Sample(s, reporter_num, len(v['input'])) for s, v in sample_ids.items()}

    for sample_id, *output in samples:
        sample_dic[sample_id].add_data(*output)


    all_correlations = []

    for sample in sample_dic.values():
        sample.data_process()
        correlations = sample.calculate_correlation()
        all_correlations += correlations

    db.new_table('sample_correlations', sample.output_ids, sample.output_dtypes)
    db.insert('sample_correlations', sample.output_ids, all_correlations)




    
def plot_correlations(db, data_id, outpath, sample_ids = None, read_filter = 100, cmap = 'viridis', setmax = None, maxv = None):

    if sample_ids is None:
        sample_ids = [s[0] for s in db.cursor.execute('Select sample_id from samples').fetchall()]

    for sid in sample_ids:
        replicate_names = db.cursor.execute(f'SELECT group_concat(distinct replicate_name) FROM replicates WHERE sample_id == "{sid}" ORDER BY replicate_name').fetchone()[0].split(',')
        replicate_number = len(replicate_names)
        reporter_num = db.reporter_num
        translation_array = np.zeros([reporter_num, replicate_number])

        q = db.cursor.execute(f'SELECT reporter_id, group_concat({data_id}) FROM data where sample_id = "{sid}"  and  input >= {read_filter} group by reporter_id having count(replicate_name) = {replicate_number} ORDER BY replicate_name').fetchall()    
        for rid, translation in q:
            translation_array[rid, :] = [float(t)for t in translation.split(',')]

        translation_array = translation_array[(translation_array > 0).sum(axis  = 1) == replicate_number]

        for idx_A, idx_B in itertools.combinations(range(replicate_number), 2):
            rname_A, rname_B = replicate_names[idx_A], replicate_names[idx_B]
            fig, ax = plotter.plot_scatter(x  = translation_array[:, idx_A], y = translation_array[:, idx_B], xlabel = replicate_names[idx_A], ylabel = replicate_names[idx_B], cmap = cmap, maxv = maxv)
            fig.suptitle(f'{sid} rfilter: {read_filter} n: {translation_array.shape[0]}', fontsize = 6)
            tmp_fname = f'replicate_correlation_{sid}_{rname_A}_{rname_B}.svg'
            fig.savefig(outpath + tmp_fname, dpi = 1200)