# NaP-TRAP Reveals the Regulatory Grammar in 5'UTR-Mediated Translation Regulation During Z6ebrafish Development

![](doc/ntrap_logo.png)

This repository details the analyses presented in "_NaP-TRAP Reveals the Regulatory Grammar in 5'UTR-Mediated Translation Regulation During Zebrafish Development_" (https://doi.org/10.1101/2023.11.09.566434).

## Overview

* `database`
    - [db_to_sqlite.py](src/database/db_to_sqlite.py): Utilizes sqlite3 module to make sqlite database queries 
    - [mpra_db.py](src/database/mpra_db.py)

* `db_features`
    - [kmer_counter.py](src/db_features/kmer_counter.py)
    - [orf_finder.py](src/db_features/orf_finder.py)
    - [hot_encoded.py](src/db_features/hot_encoded.py)
    - [structure.py](src/db_features/structure.py)

* `db_analysis`
    - [enrichment.py](src/db_analysis/enrichment.py) 
    - [feature_correlation.py](src/db_analysis/feature_correlation.py)
    - [model.py](src/db_analysis/model.py)


build_toml parameters 

* To build a new `MPRA_DB` you should supply the following paths and constants to your `build_db.toml`.

```
[paths]
db_path = 'output/utr5_fish/utr5_fish.db'             #path to schema of database
schema_path = 'doc/db_schema.sql'                     #path to sql schema
output_path = 'output/utr5_fish/'                     #path to output directory 
fasta_path = 'libraries/utr5_fish/reporters.fa'       #path to fasta containing insert sequence
selector_path = 'libraries/utr5_fish/selector.toml'   #path to selector.toml 

[constants]
adaptor_5p = 'GAATACAAGCCCTACACGACGCTCTTCCGATCT'      #constant 5' region upstream of the insert sequence
adaptor_3p = 'GTAAACATGGTGAGCAAGGGCGAGGACTACAAAGAC'   #constant 3' regoing downstream of the insert sequence (shortened for brevity)
main_orf_start = 6                                    #start of the main open reading frame can be a positive or negative integer relative to start of 3' adaptor
kozak_score_path = 'doc/kozak.json'                   #path to kozak score
```

* To add a sample to the database you need the following:

```
[data.count_paths]
ntrap_utr5_fish_pa_sv40 = 'data/utr5_fish/ntrap_utr5_fish_pa_sv40_counts.json'      # experiment_name = count path

[data.samples.pa_2hpf]                                                              # sample name should be unique
experiment_name = 'ntrap_utr5_fish_pa_sv40'                                         # experiment name must be present in [data.count_paths] 
collection_time = 2                                                                 # time of library collection
library = '60A'                                                                     # notes on libray preparation 
organism = 'Danio rerio'                                                            # organism utilized for experiment

[data.samples.pa_2hpf.runs]                                                         
AGR003825 = {replicate_name = 'B1', run_type = 'input'}                             # run_names must correspond to columns in count table, runs from the same IP should have the same replicate_name
AGR003826 = {replicate_name = 'B2', run_type = 'input'}                             # replicate_names and run_types are flexible, however must be kept consistent through out.
AGR003829 = {replicate_name = 'B1', run_type = 'flag_pulldown'}
AGR003830 = {replicate_name = 'B2', run_type = 'flag_pulldown'}
```

* To calculate NaP-TRAP derived translation scores you must include `[data.fuctions]` in your `build_db.toml`:

```
[data.functions]
calculate_delta = [{samples = ['pa_2hpf','sv40_2hpf','pa_6hpf','sv40_6hpf','hek293t_12h'], num_run_type = 'flag_pulldown', denom_run_type = 'input', data_type = 'translation'}]
```

### Setup 

- To download count data and reporter library sequences from the Giraldez lab website, run `get_data.py`. 
- Next to build databases for each of the reporter libraries, run [`build_all.py`](build_all.py).

### Requirements

Python Package versions (python 3.12.6):

- `numpy==2.1.1`
- `matplotlib==3.9.2`
- `matplotlib-venn==1.1.1`
- `pandas==2.2.2`
- `pyfaidx==0.8.1.2`
- `scikit-learn==1.5.2`
- `scipy==1.14.1`
- `toml==0.10.2`

Structure and streme analysis require additional software:

- ViennaRNA Version 2.7.0 (https://www.tbi.univie.ac.at/RNA/) was utilized for RNA [`structure analyses`](src/db_features/structure.py).
- The MEME Suite 5.5.5 (https://meme-suite.org/meme/) was utilized to generate [`motifs`](src/db_analysis/meme.py).