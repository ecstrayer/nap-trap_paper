#!/usr/bin/env python

import argparse
import json 

class Umi_counter:

    umi_len = 10
    thres = 1

    def __init__(self, seq_list):

        self.seq_list = seq_list
        self.umi_dic = {}

        i = 0
        
        for seq in self.seq_list:
            if seq in self.umi_dic:
                self.umi_dic[seq].read_count += 1
            else:
                self.umi_dic[seq] = Umi(seq,i)
                i += 1
 
        #because we are using 1 as a hamming distance by definition if two sequences are not next to each other on either the forward or reverse sorted they have a hamming distance greater than 1

        self.umi_fsorted = sorted(self.umi_dic.values(),key = lambda a:a.seq)
        self.cluster_umi(self.umi_fsorted)
        self.umi_rsorted = sorted(self.umi_dic.values(),key = lambda a:a.seq[::-1])
        self.cluster_umi(self.umi_rsorted)

    def hd(self,x,y):
        
        d = 0
        
        for bx,by in zip(x,y):
            if bx == by:
                continue

            elif d > self.thres:
                return False

            else:
                d += 1
        
        return True 

    
    def cluster_umi(self,sorted_list):

        umi2 = Umi('',-1)

        for umi1 in sorted_list:

            if self.hd(umi1.seq,umi2.seq):
                
                if umi1.read_count >= umi2.read_count:
                    umi1.read_count += umi2.read_count
                    _ = self.umi_dic.pop(umi2.seq, None)
                    umi2 = umi1

                else:
                    umi2.read_count += umi1.read_count
                    _ = self.umi_dic.pop(umi1.seq, None)
                    
    @property
    def total_count(self):
        return len(self.umi_dic.keys())


class Umi:

    def __init__(self,seq,idx):

        self.seq = seq
        self.read_count = 1



#####################################################################################################################################################

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-i',type = str,help = 'input_file_path')
    args = parser.parse_args()

    input_path = args.i
    output_path = input_path.strip('_rawcounts.json')
    input_count = json.load(open(input_path,'r'))

    count_dic = {}

    for oligo_id, umi_list in input_count.items():
        umi_counter = Umi_counter(umi_list)
        count_dic[oligo_id] = umi_counter.total_count

    json.dump(count_dic,open(f'{output_path}_counts.json','w'))

