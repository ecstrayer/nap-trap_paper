import json
import scipy.sparse
import numpy as np
import toml
import sqlite3

from mpradb.database import selector,db_to_sqlite


class MPRA_DB(db_to_sqlite.DB_SQLite):
    
    def __init__(self, db_path, output_path, schema_path = None):
        super().__init__(db_path, schema_path)
        self.output_path = output_path

        self.selector_names = []
        self.feature_names = {}


    def add_selectors(self, selector_path):

        self.selector_path = selector_path
        self.selector_lookup = toml.load(self.selector_path)['selectors']
        self.all_data_group_ids = set()


        for selector_name, params in self.selector_lookup.items():
            params['selector_name'] = selector_name
            params['selector_path'] = selector_path
            s = selector.Selector(params = params, db = self)
            setattr(self,selector_name, s)            
            s.get_read_filtered_reporter_ids(self)

            reporter_ids = s.reporter_ids
            data_group_ids = s.data_group_ids
            selector_id = self.get_selector(reporter_ids = reporter_ids, data_group_ids = data_group_ids, reporter_group_name = selector_name,description = json.dumps(params))

            s.selector_id = selector_id
            self.all_data_group_ids.update(data_group_ids)
            self.selector_names.append(selector_name)
    
        self.all_data_group_ids = tuple(sorted(self.all_data_group_ids))
        self.load_data()
        
    def load_data(self):
        _, self.data_group_names, self.data = self.select(columns = ['reporter_id','data_group_id', 'data_value']).where(self['data_group_id'].in_(self.all_data_group_ids)).to_numpy(column_ids = self.all_data_group_ids, column_key = 'data_group_name')


    def load_features(self, feature_ids, sparse = False):
        if feature_ids == 'all':
            _, self.feature_idxs, self.features = self.select(columns = ['reporter_id','feature_id', 'feature_value']).to_numpy(column_key = 'feature_name')
        else:
            _, self.feature_idxs, self.features = self.select(columns = ['reporter_id','feature_id', 'feature_value']).where(self['feature_id'].in_(feature_ids)).to_numpy(column_ids = feature_ids, column_key = 'feature_name')

        self.feature_names = {v:k for k,v in self.feature_idxs.items()}

    @property
    def selectors(self):
        return [getattr(self,s) for s in self.selector_names]



    def add_parameters(self, db_params : dict):
        pass


    @property
    def max_sample_id(self):

        max_sample_id = self.cursor.execute('Select MAX(sample_id) from mpra_sample').fetchall()[0][0]
        max_sample_id = -1 if max_sample_id is None else max_sample_id
        return max_sample_id


    @property
    def max_replicate_id(self):
        
        max_replicate_id = self.cursor.execute('Select MAX(replicate_id) from mpra_replicate').fetchall()[0][0]
        max_replicate_id = -1 if max_replicate_id is None else max_replicate_id
        return max_replicate_id



    def get_distinct(self, columns : list):
        self.distinct = True 
        return  set([s[0] if len(s) == 1 else s for s in self.select(columns = columns).fetchall()])



    def get_run_groups(self):

        rgroup = self.select(columns = ['run_group_id','run_id']).groupby(['run_group_id'], aggregate_functions = {'group_concat' : ['run_id']}).fetchall()
        run_group_lookup = {frozenset([int(r) for r in rids.split(',')]) : rgid for rgid, rids in rgroup} 

        return run_group_lookup



    def add_features(self, feature_names, reporter_ids,feature_types, feature_data):

        '''
        This method adds features to the database. Feature names and types are stored in the feature_attr table, whereas the actual values of said feature are stored in the feature table.
        '''

        #adding feature names and types to feature_attr table

        max_feature_id = self.max_feature_id
        fid_lookup = {}
        fdata = []
    
        new_features = []

        for fname, ftype, rid, fval in zip(feature_names, feature_types, reporter_ids, feature_data):
            if fname not in fid_lookup:
                max_feature_id += 1
                fid_lookup[fname] = max_feature_id
                new_features.append([max_feature_id, fname, ftype])
            
            fid = fid_lookup[fname]
            fdata.append([fid, rid, fval])

        self.insert('feature_attribute', ['feature_id', 'feature_name', 'feature_type'], new_features)

        #adding feature values to feature table

        self.insert('feature', ['feature_id', 'reporter_id', 'feature_value'], fdata)


    #sql_result should be orded (row, col, data)

    def make_csc(self, sql_result, feature_num):

        rows, cols, data, fidx = [], [], [], []

        for rid, cid, vals in sql_result:
            rid = [int(r) for r in rid.split(',')]
            rows += rid
            cols += [cid for n in range(len(rid))]
            data += [float(d) for d in vals.split(',')]
            fidx.append(cid) 

    
        rows,cols,data = np.array(rows), np.array(cols), np.array(data)  
        sp_mat = scipy.sparse.csc_matrix((data, (rows, cols)), shape = (self.reporter_num, feature_num))

        return sp_mat,fidx



    def get_data(self, sel_hash, data_type):

        selector_lookup = self.select(['reporter_group_hash', 'reporter_group_id']).to_dict()

        if sel_hash not in selector_lookup:
            raise Exception('Please use a valid selector.')
        

        selector_id = selector_lookup[sel_hash]
        reporter_ids = self.select(columns = ['reporter_id']).where(self['selector_id'] == selector_id).fetchall()
        data_group_ids = self.select(columns = ['data_group_id']).where(self['selector_id'] == selector_id).fetchall()

        rdic, cdic, X = self.select(columns = ['reporter_id','data_group_id','data_value']).where((self['reporter_id'].in_(reporter_ids)) & (self['data_type'] == data_type) & (self['data_group_id'].in_(data_group_ids))).to_numpy(row_ids = reporter_ids ,column_ids = data_group_ids)


        return rdic, cdic, X


    def get_selector(self, reporter_ids, data_group_ids, description, reporter_group_name, reporter_group_type = 'selector', reporter_group_parent_id = None):

        sel_str = str(sorted(reporter_ids) + sorted(data_group_ids))
        sel_hash = self.get_hash(sel_string = sel_str)
        selector_lookup = self.select(['reporter_group_hash', 'reporter_group_id']).to_dict()
        
        if sel_hash not in selector_lookup:
            max_selector_id = self.get_max('reporter_group_id') + 1
            rids_to_add = [tuple([max_selector_id, r]) for r in reporter_ids]
            dids_to_add = [tuple([max_selector_id, d]) for d in data_group_ids]
            self.insert(table_name = 'reporter_group_attribute', columns = ['reporter_group_id','reporter_group_type', 'reporter_group_hash', 'reporter_group_name', 'reporter_group_description'], values = [max_selector_id, reporter_group_type, sel_hash, reporter_group_name, description], single = True)
            self.insert(table_name = 'reporter_group_to_reporter_iso', columns = ['reporter_group_id', 'reporter_id'], values = rids_to_add)
            self.insert(table_name = 'reporter_group_to_data_group_iso', columns = ['reporter_group_id', 'data_group_id'], values = dids_to_add)

            if reporter_group_parent_id is not None:
                self.insert(table_name = 'reporter_group_to_reporter_group_iso', columns = ['reporter_group_id','reporter_group_parent_id'], values = [max_selector_id, reporter_group_parent_id], single= True)

        else:
            max_selector_id = selector_lookup[sel_hash]

        return max_selector_id


    def get_reporters(self, reporter_subset):

        reporter_subset_str = '"' + '","'.join(reporter_subset) + '"'
        rids = self.cursor.execute(f'SELECT reporter_id from reporters where reporter_subtype in ({reporter_subset_str})').fetchall()
        return set([r[0] for r in rids])



    def get_features(self, feature_name = None, feature_types = None):
        pass



    def get_feature_slice(self, feature_name):

        reporter_num = self.reporter_num
        fslice = np.zeros(reporter_num)

        try:
            rids, fvals = self.cursor.execute(f'SELECT reporter_id, value FROM features JOIN feature_attr USING (feature_id) WHERE feature_name = "{feature_name}"').fetchone()
            rids = [int(r) for r in rids.split(',')]
            fvals = [float(f) for f in fvals.split(',')]

            for r,v in zip(rids,fvals):
                fslice[r] = v
                
        except:
            pass
        
        return fslice
            

    def get_reporter_fselectors(self, feature_names, sel_string):

        feature_eval = {fname: self.get_feature_slice(fname) for fname in feature_names}
        sel_string = sel_string.replace(' and ',' * ').replace(' or ',' + ')
        selector = eval(sel_string, feature_eval)
        selector = set(selector.nonzero()[0].tolist())

        return selector


    def add_minumum_reads(self, sample_ids, minumum_reads):

        self.check_samples(sample_ids)

        if len(sample_ids) != len(minumum_reads):
            raise Exception('Please provide a read_filter for each sample_id.')


        self.cursor.execute('BEGIN TRANSACTION;')

        try:

            for sid, minread in zip(sample_ids, minumum_reads):
                self.cursor.execute(f'UPDATE samples set read_filter = {minread} WHERE sample_id = "{sid}"')
                                  
            self.cursor.execute('COMMIT')

        except sqlite3.Error as e:
            self.cursor.execute('ROLLBACK')
            raise Exception(f' Exception: {" ".join(e.args)}')
            
        

    def get_minumum_reads(self, sample_ids):

        self.check_samples(sample_ids = sample_ids)
        sample_lookup = {sample_id: read_filter for sample_id, read_filter in self.cursor.execute('SELECT sample_id, read_filter FROM samples').fetchall()}

        minumum_reads = []
        samples_not_added = []

        for s in sample_ids:
            read_filter = sample_lookup[s]

            if read_filter == -1:
                samples_not_added.append(s)
            else:
                minumum_reads.append(read_filter)
            
        
        if len(samples_not_added) != 0:
            raise Exception(f'Please add read_filters to {samples_not_added} in samples table.')
    
        return minumum_reads



    def check_samples(self, sample_ids):

        samples_in_db = [s[0] for s in self.cursor.execute('SELECT sample_id FROM samples').fetchall()]
        samples_not_found = [s for s in sample_ids if s not in samples_in_db] 

        if len(samples_not_found) != 0:
            raise Exception(f'Sample_ids: {samples_not_found} not found in db. Please add samples!')
            
        else:
            return 

    @property
    def constants(self):
        return {cname:cvalue for cname, cvalue in self.select(columns = ['constant_name','constant_value']).fetchall()}

    def get_constants(self, constants_):
        allc = self.constants
        ctoget = []

        for c in constants_:
            if c in allc:
                #add try except for float
                v = allc[c]
                try:
                    v = float(v)
                except:
                    pass

                ctoget.append(v)
            else:
                raise Exception(f'Please add {c} to mpra_constant table!')

        return ctoget


    @property
    def reporters(self):
        return self.select(['reporter_id','insert_sequence']).where(self['tags'].like('reporter')).fetchall()

    '''
    @property
    def feature_names(self):
        return [f[0] for f in self.cursor.execute('SELECT feature_name FROM feature_attr').fetchall()]
    '''
    #Not sure if I want feature table yet

    @property
    def max_feature_id(self):
        feature_num = self.cursor.execute('SELECT max(feature_id) FROM feature_attribute').fetchall()
        if feature_num[0][0] is None:
            return -1
        else:
            return feature_num[0][0] 
        

    @property
    def kozak_lookup(self):
        if 'kozak' not in self.tables:
            raise Exception('Kozak frequency matrices not found. Run kozaks.add_kozak_scores first')
        else:
            kozak_dic = self.cursor.execute('SELECT kozak_id, frequency_matrix FROM kozak').fetchall()
            return {k: json.loads(v) for k,v in kozak_dic}


    @property
    def reporter_num(self):
        return self.select(columns = ['reporter_id']).add_func_to_select({'MAX':['reporter_id']}).fetchone()

    @property
    def reporter_names(self):
        return self.select(['reporter_name']).to_list()
