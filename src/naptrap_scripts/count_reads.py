#!/usr/bin/env python

import argparse
import datetime
import glob
import multiprocessing
import json
import os
import subprocess

paired_end = False
tmp_path = None
today = datetime.datetime.today().strftime('%m%d%y')



class Aligned_Read:

    __slots__ = ('read_id', 'umi', 'flags', 'reference_name', 'start', 'mapq', 'cigar_str', 'mate_reference_name', 'mate_start', 'inferred_size', 'sequence', 'quality_score', 'AS', 'XS', 'YS', 'XN', 'XM', 'XO', 'XG', 'NM', 'YF', 'YT', 'MD', 'matches', 'read_full', 'edit_distance', 'min_matches')

    def __init__(self):

        self.read_full = False

    def add_read(self, l):

        l = l.strip('\n').split('\t')
        read_id = l[0].split('#')
        self.read_id = read_id[0]
        self.umi = read_id[1]
        self.flags = int(l[1])
        self.reference_name = l[2]
        self.start = l[3]
        self.mapq = l[4]
        self.cigar_str = l[5]
        self.mate_reference_name = l[6]
        self.mate_start = l[7]
        self.inferred_size = l[8]
        self.sequence = l[9]
        self.quality_score = l[10]
        self.add_tags(l[11:])
        self.matches = 0
        self.read_cigar()
        self.read_full = True

    def read_cigar(self):

        cigar_list = list(self.cigar_str)

        i = 0

        while len(cigar_list) > 0:
            b = cigar_list[i]
            if b in 'MIDNSHP=X':
                if b == 'M':
                    n = cigar_list[:i]
                    self.matches += int(''.join(n))
                cigar_list = cigar_list[i+1:]
                i = 0
            i += 1


    def add_tags(self, tags):

        for t in tags:
            t = t.split(':')
            k = t[0]
            var_type = t[1]
            v = t[2]
            
            if var_type == 'f':
                v = float(v)    
            elif var_type == 'i':
                v = int(v)

            setattr(self,k,v)

    def read_passed(self):

        if self.edit_distance <= self.NM or self.matches <= self.min_matches:
            return False
        else:
            return True
        
    def __bool__(self):
        return self.read_full




class ReadPair:


    edit_distance_r1 = 100
    min_matches_r1 = 0
    edit_distance_r2 = 100
    min_matches_r2 = 0


    def __init__(self):
        self.read1 = None
        self.read2 = None
        self.read_full = False
    
    def add_read(self, read):
    
        if self.read1 == None:
            self.read1 = Aligned_Read().add_read(read)
        elif self.read2 == None:
            self.read2 = Aligned_Read().add_read(read)
            self.read_full = True
        else:
            raise Exception('Read pair already full')

    def read_passed(self):

        if self.read1 == None or self.read2 == None:
            raise Exception('Read pair not full')
        
        if self.read1.read_id != self.read2.read_id:
            raise Exception(f'Read id does not match: {self.read1.read_id}, {self.read2.read_id}')
        
        if self.read1.reference_name != self.read2.reference_name:
            raise Exception(f'Reads do not map to same reference {self.read1.reference_name}, {self.read2.reference_name}')
        
        return self.read1.read_passed(self.edit_distance_r1, self.min_matches_r1) and self.read2.read_passed(self.edit_distance_r2, self.min_matches_r2)
        
    def __bool__(self):
        return self.read_full

    
    @property
    def reference_name(self):
        return self.read1.reference_name

    @property
    def umi(self):
        return self.read1.umi





def read_accepted_hits(path_run_id):

    fpath, run_id = path_run_id
    ftype = '.'.join(fpath.split('.')[1:])
    reporter_dic = {}
    
    sproc_dict = {
        'bam' : ['samtools','view', fpath],
        'sam' : ['cat', fpath],
        'sam.zst' : ['zstdcat', fpath]      
    }

    if ftype not in sproc_dict:
        raise Exception(f'File type {ftype} not valid')

    proc = subprocess.Popen(sproc_dict[ftype], stdout= subprocess.PIPE)

    mapped_read = ReadPair() if paired_end else Aligned_Read()

    for l in proc.stdout:
        l = l.decode('utf-8')
        if l.startswith('@'):
            if l.startswith('@PG'):
                bowtie2_cl = l.split('CL:')[-1]
                paired_end_status = '-1' in bowtie2_cl and '-2' in bowtie2_cl 
                if paired_end_status != paired_end:
                    raise Exception(f'The {ftype} file does not match the paired end specification. Please review the command used to generate the mapped reads {bowtie2_cl}.')        
        else:
            mapped_read.add_read(l)    

            try:
                if mapped_read:
                    if mapped_read.read_passed():
                        reporter_id = mapped_read.reference_name
                        if reporter_id not in reporter_dic:
                            reporter_dic[reporter_id] = []
                        reporter_dic[reporter_id].append(mapped_read.umi)
                    mapped_read = ReadPair() if paired_end else Aligned_Read()

            except Exception as e:
                if paired_end:
                    print(e, f'{e}, file: {fpath}, read_id: {mapped_read.read1.read_id}, {mapped_read.read2.read_id}')
                    mapped_read.read1 = mapped_read.read2
                    mapped_read.read2 = None
                continue

    file_name = f'{tmp_path}{run_id}_{today}_rawcounts.json'
    json.dump(reporter_dic, open(file_name,'w'))
    print(reporter_dic)

    return file_name



def read_fasta(fpath):

    reporter_dic = {}

    f = subprocess.Popen(['zstd', '-d', '-f', '-c',fpath], stdout= subprocess.PIPE)
    f = f.stdout 

    run_name = fpath.split('/')[-1].split('_')[0]
    print(run_name)

    for i,l in enumerate(f):
        l = l.decode('utf-8')

        if i % 4 == 1:
            reporter_id = l.strip('\n')
            
            if reporter_id in reporter_dic:
                reporter_dic[reporter_id].append(umi)   
            else:
                reporter_dic[reporter_id] = [umi]


        elif l.startswith('@'):
            umi = l.split('#')[-1]

    f.close()

    file_name = f'{output_path}{run_name}_{today}_rawcounts.json'
    json.dump(reporter_dic, open(file_name,'w'))

    return file_name


def launch_count_umi(input_path):

    subprocess.run(['count_unique','-i',input_path], check = True)  
    output_path = input_path.strip('_rawcounts.json')
    output_path = output_path + '_counts.json'
    sample_name = output_path.split('/')[-1].split('_')[0]
    output_file = json.load(open(output_path,'r'))


    os.remove(input_path)
    os.remove(output_path)

    return sample_name,output_file


def path_test(p):

    if not os.path.exists(p):
        os.makedirs(p)

    return p


def merge_dic(results):

    assembled_dic = {}

    for sample_name,d in results:
        assembled_dic[sample_name] = d

    return assembled_dic


def get_unique_run_ids(path_list, idx = -1):

    number_of_files = len(path_list)
    new_pl = [f.split('.')[0].split('/')[idx] for f in path_list]
    
    if len(set(new_pl)) != number_of_files:
        path_lookup = get_unique_run_ids(path_list = path_list, idx = idx - 1)
    else:
        path_lookup = [(path,run_id) for path,run_id in zip(path_list,new_pl)]

    return path_lookup





#########################################################################################################


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-i',type = str,help = 'regex for pipseq files')
    parser.add_argument('-o',type = str,help = 'output path')
    parser.add_argument('-e',type = str,help = 'experiment id')
    parser.add_argument('-t',type = str, help = 'temporary directory')
    parser.add_argument('-p',type = int, help = 'number of processors')
    parser.add_argument('--paired', action = 'store_true', help = 'does the sam file contain paired end mapped reads')
    parser.add_argument('-d1',type = int, default= 100, help = 'edit distance')
    parser.add_argument('-m1', type = int, default = 10, help = 'minimum matches')
    parser.add_argument('-d2',type = int, default= 100, help = 'edit distance')
    parser.add_argument('-m2', type= int, default = 10, help = 'minimum matches')

    args = parser.parse_args()
    
    input_regex = args.i
    output_path = path_test(args.o)
    experiment_id = args.e
    proc_num = args.p
    tmp_path = path_test(args.t)

    paired_end = args.paired

    Aligned_Read.edit_distance = args.d1
    Aligned_Read.min_matches = args.m1

    ReadPair.edit_distance_r1 = args.d1
    ReadPair.min_matches_r1 = args.m1
    ReadPair.edit_distance_r2 = args.d2
    ReadPair.min_matches_r2 = args.m2

    input_regex = input_regex.replace('.zstd','')
    ftype = '.'.join(input_regex.split('.')[1:])


    path_list = [f for f in glob.glob(input_regex) if not 'undet' in f]
    path_run_ids = get_unique_run_ids(path_list = path_list)

    #need to fix this ...

    with multiprocessing.Pool(proc_num) as pool:
        if ftype == 'sam.zst' or ftype == 'bam' or ftype == 'sam':
            results = pool.map(read_accepted_hits,path_run_ids)  
        elif ftype == 'fasta':
            results = pool.map(read_fasta,path_run_ids)
        else:
            raise Exception(f'File type {ftype} not valid')

        results = pool.map(launch_count_umi,results)   
     

    reporter_dic = merge_dic(results)

    json.dump(reporter_dic, open(f'{output_path}{experiment_id}_{today}_counts.json','w'))

if __name__ == '__main__':
    main()