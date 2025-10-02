import json
import numpy as np
import pandas as pd
import argparse
import os
import matplotlib.pyplot as plt
import seaborn as sns
def read_cutoff_decision(count_path,out_path,fig_format=None,data_name = None):

    if not fig_format:
        fig_format = 'svg'
    
    fig_save_path = f"{out_path}/figures/"

    if not os.path.exists(fig_save_path):
        os.makedirs(fig_save_path)


    cutoff_list = [1,5,10,25,50,100]
    count_dict = json.load(open(count_path,'r'))
    result_dic = {}
    for data,counts in count_dict.items():
        print(f" Processing {data}...")
        if ((data_name) and (data_name != data)):
            continue
        npc = []
        for n,c in counts.items():
            if 'spike' not in n:
                npc.append(c)
        npc = np.array(npc)
        num_rep = [np.sum(npc > cut) for cut in cutoff_list]
        result_dic[data] = num_rep

        fig,ax = plt.subplots(1,1)

        sns.histplot(npc,ax=ax,bins=100, log_scale=True)
        ax.set_xlabel(f"Read count of {data} data")
        ax.set_ylabel("Number of reporters")
        
        fig.savefig(f"{fig_save_path}read_vs_count_histogram_{data}.{fig_format}")

    result_df = pd.DataFrame(result_dic, index = cutoff_list).T.reset_index()
    result_df.to_csv(f"{out_path}/Cutoff_vs_Count_table.csv")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i',type = str,help = 'count file path')
    parser.add_argument('-o',type = str,help = 'output directory')
    parser.add_argument('-f',type = str,help = 'figure save format',default='svg')
    parser.add_argument('-d',type = str,help = 'data name to analyse',default=None)
    args = parser.parse_args()

    input_path = args.i
    output_path = args.o
    fig_format = args.f
    data_name = args.d

    read_cutoff_decision(count_path=input_path,out_path=output_path,fig_format=fig_format,data_name=data_name)



if __name__ == '__main__':
    main()