import numpy as np
import matplotlib.font_manager
import matplotlib.pyplot as plt
import subprocess
import os
import sys


from mpradb.database import mpra_db
from mpradb.db_plot import plotter


class Motif:

    nucleotides = ['A','C','G','U']

    def __init__(self, motif, outpath, motif_probabilty = None, evalue = None, background_frequency = None, number_of_sites = None):
        self.consensus = motif
        self.motif_len = len(motif)
        self.figure_path = outpath
        self.motif_probability = np.zeros([self.motif_len, 4]) if motif_probabilty is None else motif_probabilty
        self.pos = -1
        

        self.evalue = evalue
        self.background_frequency = background_frequency
        self.number_of_sites = number_of_sites

    def add_motif(self, l):
        
        if l.startswith('letter'):
            l = l.split(':')[-1].replace('= ','=').split()
            for x in l:
                k,v = x.split('=')
                v = v.strip()
                if k == 'nsites':
                    setattr(self, 'number_of_sites', int(v))
                elif k == 'E':
                    setattr(self, 'evalue', float(v))
        else:
            try:
                l = [float(x) for x in l.split()]
                self.pos += 1
                self.motif_probability[self.pos,:] = l
            except: 
                pass

    def calculate_bits(self):

        self.PWM = np.log2(self.motif_probability / self.background_frequency)
        self.E= (1 / np.log(2)) * ((len(self.nucleotides) - 1) / self.number_of_sites)


        
        f = lambda x: x * np.log2(x) 
        self.H =  -1 * f(self.motif_probability).sum(axis = 1)
        self.information_content = np.log2(len(self.nucleotides)) - (self.H)
        self.bits = (self.motif_probability.T  * self.information_content).T

    @property
    def pstr(self):
        pstr = ''
        for c in self.motif_probability:
            c = ' '.join([str(x) for x in c])
            pstr += f'{c}\n'

        return pstr
        
 

    @classmethod
    def add_background(Motif,l):
        
        freq = []
        nucleotides = []

        l = l.strip('\n').split()
        for i in range(0, len(l),2):
            n = l[i]
            f = l[i+1]
            nucleotides.append(n)
            freq.append(float(f))

        Motif.background_frequency = np.array(freq)
        Motif.nucleotides = tuple(nucleotides)



def plot_sequence_logo(motif):

    color = ['red','blue','orange','green']
    #color = ['pink','pink','pink','pink']

    fontprop = matplotlib.font_manager.FontProperties(family='monospace')

    fig,ax = plotter.make_figure()
    motif_len, alphabet_len = motif.motif_probability.shape
    ax.set_xticks(np.arange(0.5,motif_len+0.5, 1))
    ax.set_xticklabels(range(motif_len), fontsize = 6)
    ax.set_ylim(0,2)
    ax.set_xlim(-0.1, motif_len)
    ax.set_ylabel('Bits', fontsize = 8)
    ax.set_xlabel('Position', fontsize = 8)
    #ax.annotate(motif.file_path, (0.5,-0.5), fontsize = 1, annotation_clip = False)
   
    
    fig_title = f'{motif.consensus}\nE-value: {motif.evalue}' if motif.rbp is None else f'{motif.consensus} {",".join(motif.rbp)}\nE-value: {motif.evalue}'

    fig.text(0.5, 1.1, fig_title, transform = ax.transAxes, ha = 'center', fontsize = 6)
 


    sort_motif_idx = motif.motif_probability.argsort(axis = 1)


    for i in range(motif_len):
        motif_base = 0
        for s in range(alphabet_len):
            n = sort_motif_idx[i,s]
            b = motif.bits[i,n]
            p = matplotlib.textpath.TextPath((0,0), s = motif.nucleotides[n], size = 1, prop = fontprop)
            pbox = p.get_extents()

            wscale = 0.98 / pbox.width
            hscale = b / pbox.height
            char_width = pbox.width * wscale
            #char_height = pbox.height * hscale
            char_shift = (1 - char_width) / 2
            #char_height = (b - char_height)


            transformer = matplotlib.transforms.Affine2D().translate(tx = -pbox.xmin, ty = -pbox.ymin).scale(sx = wscale, sy = hscale).translate(tx = i-char_shift, ty = motif_base)
            p = p.transformed(transformer)
            p = matplotlib.patches.PathPatch(p, color = color[n], linewidth = 0)
            ax.add_patch(p)
            motif_base += b

    return fig,ax




import glob

def parse_xstreme_output(meme_path_glob):

    meme_path_list = glob.glob(meme_path_glob)
    tomtom_outpath = meme_path_glob.split('*')[0]
    tomtom_outpath = f'{tomtom_outpath}tom_tom_all.txt'
    motifs = []

    for meme_path in meme_path_list:
        Motif.file_path = meme_path
        fig_path = '/'.join(meme_path.split('/')[:-1])
        m = None

        with open(meme_path, 'r') as f:
            for l in f:
                if l.startswith('Background'):
                    c = next(f)
                    Motif.add_background(c)
                elif l.startswith('MOTIF'):
                    if m is not None and m.evalue < 0.05:  
                        motifs.append(m)
                           
                    l = l.split()[1].split('-')[-1]
                    m = Motif(l, outpath = fig_path)
                    m.group = meme_path.split('/')[-2]
                elif m is not None:
                    m.add_motif(l)
                else:
                    continue 

        if m is not None and m.evalue < 0.05:  
            motifs.append(m)

    gdic = {}
    gnum = 0
    m = motifs[0]

    with open(tomtom_outpath, 'w') as f:
        f.write(f'MEME version 5.5.5 \n\nALPHABET= ACGU\n\nBackground letter frequencies (from uniform background):\nA {m.background_frequency[0]} C  {m.background_frequency[1]} G  {m.background_frequency[2]} U  {m.background_frequency[3]}\n')

        for m in motifs:
            if m.group not in gdic:
                gdic[m.group] = gnum
                gnum += 1

            gid = gdic[m.group]

            m.calculate_bits()
            f.write(f'\nMOTIF {m.consensus}\nletter-probability matrix: alength= 4 w= {m.motif_len} nsites= {m.number_of_sites} E= {m.evalue}\n')
            f.write(m.pstr)


    return motifs, gdic
        









def make_fasta(outpath, rseq):

    with open(outpath, 'w') as f:
        for rname, seq in rseq:
            f.write(f'>{rname}\n{seq}\n')




def rbpid_to_rbp(meme_path):

    rbp_lookup = {}

    with open(meme_path, 'r') as f:
        for l in f:
            if l.startswith('MOTIF'):
                _, k, v = l.strip('\n').split()
                rbp_lookup[k] = v
    
    return rbp_lookup




def selector_to_meme(fd, min_motif=5, max_motif=8, proc_num = 10):

    #make paths for meme enrichment
    output_path = os.path.join(fd.db.output_path, 'meme_output_5_12')
    selector_id_lookup = fd.db['reporter_group_name', 'reporter_group_id'].where(db['reporter_group_type'] == 'selector').to_dict()
    
    for selector_name in fd.selector_names:
        background_sequence = fd.db.select(columns = ['reporter_name', 'insert_sequence']).to_list()
        output_dir = os.path.join(output_path, selector_name)
        control_fasta = f'{output_dir}/control.fa'
        selector_id = selector_id_lookup[selector_name]

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            make_fasta(control_fasta, background_sequence)

        rdic = fd.db.select(columns = ['reporter_group_type','reporter_group_name','reporter_name','insert_sequence']).where(fd.db['reporter_group_parent_id'] == selector_id).to_dict(group_by_key=True, key = ['reporter_group_type','reporter_group_name'])
        for egroup, rseq in rdic.items():
            egroup_name = '-'.join(egroup)
            selector_outdir = os.path.join(output_dir, egroup_name)
            fasta_path = f'{output_dir}/{egroup_name}.fa'
            make_fasta(fasta_path, rseq)
            tomtom_dir = os.path.join(selector_outdir, 'tomtom_output')
            subprocess.run(['meme-streme', '-p', fasta_path, '-o', selector_outdir,'--minw', str(min_motif), '--maxw', str(max_motif), '--thresh', '0.05', '-evalue', '-n', control_fasta, '--rna', '--verbosity', str(1), '--align', 'right'], check= True)
            subprocess.run(['meme-streme_xml_to_html', f'{selector_outdir}/streme.xml', f'{selector_outdir}/streme.html'])

        tomtom_dir = os.path.join(output_dir, 'tomtom_output')
        motif_list,gdic = parse_xstreme_output(f'{output_dir}/*/streme.txt')            
        subprocess.run(['meme-tomtom','-verbosity','1', '-evalue', '-thresh', '10', '-oc', tomtom_dir,  f'{output_dir}/tom_tom_all.txt', '/plus/scratch/users/ethan/meme_motif_database/motif_databases/RNA/Ray2013_rbp_Homo_sapiens.meme'])
        
        df = pd.read_csv(f'{tomtom_dir}/tomtom.tsv',sep = '\t',comment ='#')
        rbp_lookup = rbpid_to_rbp('/plus/scratch/users/ethan/meme_motif_database/motif_databases/RNA/Ray2013_rbp_Homo_sapiens.meme')
        df['rbp'] = df['Target_ID'].apply(lambda a:rbp_lookup[a])
        df = df[df['E-value'] < 0.05]
        rbp_motif = {}
        for _, c, r in df.loc[:,['Query_ID','rbp']].itertuples():
            if c not in rbp_motif:
                rbp_motif[c] = []
            rbp_motif[c].append(r)

        rbp_motif = {k:set(v)  for k,v in rbp_motif.items()}


        for m in motif_list:
            m.rbp = rbp_motif.get(m.consensus)
            fig,ax = plot_sequence_logo(m)
            fig_group= f'{tomtom_dir}/{m.group}'
            if not os.path.exists(fig_group): os.makedirs(fig_group)
            fig.savefig(f'{fig_group}/{m.consensus}.svg')
            plt.close()










if __name__ == '__main__':

    meme_path = '/home/ethan/scratch/meme_motif_database/motif_databases/RNA/Ray2013_rbp_Homo_sapiens.meme'
    rbp_lookup = rbpid_to_rbp(meme_path)

    db = mpra_db.MPRA_DB(db_path = '/plus/fast_scratch/users/ethan/ntrap_newstruct_test.db', schema_path = '/home/ethan/new_work/code/python/nap-trap_paper/sql/ntrapv4.sql',output_path = '/home/ethan/new_work/tmp/ntrap_db_output/')
    fd = feature_data.Feature_Data(db=db)
    fd.add_selectors('/home/ethan/new_work/code/python/nap-trap_paper/doc/selector_test.toml')
    fd.add_data()
    selector_to_meme(fd,5,12,20)