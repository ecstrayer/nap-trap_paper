import numpy as np

class Selector:

    def __init__(self, db, params):
        self.sample_names = params['sample_names']
        self.data_types = params['data_types']
        self.read_filters = params['read_filters']
        self.features_to_exclude = params['features_to_exclude']
        self.features_to_include = params['features_to_include']
        self.filter_features = params['filter_features'] 
        self.reporter_ids = []
        self.db = db

    def get_read_filtered_reporter_ids(self, db):

        self.data_group_lookup = db.select(['data_group_name', 'data_group_id']).where((db['sample_name'].in_(self.sample_names)) & (db['data_group_type'].in_(self.data_types))).to_dict()
        self.data_group_ids = list(self.data_group_lookup.values())
        self.read_filter_data_ids = []
        reporter_ids_list = []
        filters_added = 0

        for run_type, raw_data_type, rf in self.read_filters:
            run_ids = set(db.select(['run_id']).where((db['run_type'] == run_type) & (db['data_group_id'].in_(self.data_group_ids))).to_list())
            num_run_ids = len(run_ids)
            reporter_lookup = db.select(['reporter_id', 'replicate_id']).where((db['run_id'].in_(run_ids)) & (db[raw_data_type] >= float(rf)) & (db['tags'].like('%spikein%',exclude = True))).to_dict(group_by_key = True)
            reporter_ids_list.append(set([r for r,d in reporter_lookup.items() if len(d) == num_run_ids]))  
            self.read_filter_data_ids.append(tuple([run_type, tuple(run_ids)]))
            filters_added += 1
        
        if filters_added == 0:
            raise Exception('Read filter failed.')
        elif filters_added == 1:
            reporter_ids_to_add = reporter_ids_list[0]
        else:
            reporter_ids_to_add = set()
            for i,s in enumerate(reporter_ids_list):
                if i == 0:
                    reporter_ids_to_add = s
                else:
                    reporter_ids_to_add.intersection(s)
            
        self.update_reporter_ids(reporter_ids_to_add)


    def get_feature_filtered_reporter_ids(self, db):

        try:
            reporter_ids_to_add = set(db.select(columns = ['reporter_ids']).where(self.filter_features).to_list())
        except:
            raise Exception(f'Feature selector not vaild {self.filter_features}')
        
        self.update_reporter_ids(reporter_ids_to_add)


    def get_feature_ids(self, db):
        self.feature_ids = db.select(['feature_id']).where((db['feature_name'].in_(self.features_to_include)) & (db['feature_name'].in_(self.features_to_exclude, exclude = True)))


    def update_reporter_ids(self, reporter_ids_to_add : set):
        if len(self.reporter_ids) != 0:
            updated_reporter_ids = set(self.reporter_ids).intersection(reporter_ids_to_add)       
        else:
            updated_reporter_ids = reporter_ids_to_add

        self.reporter_ids = sorted(list(updated_reporter_ids))


    @property
    def data(self):
        idx = [self.db.data_group_names[d] for d in self.data_group_lookup]
        return self.db.data[self.reporter_ids,:][:,idx]

    @property
    def features(self):
        return self.db.features[self.reporter_ids,:]



    def __getitem__(self, i):

        fidx, didx = [],[]

        if len(i) == 2:
            ridx, cnames = i
            if type(ridx) is slice:
                ridx = self.reporter_ids
            else:
                ridx = sorted(set(self.reporter_ids).intersection(set(ridx)))
                if len(ridx) == 0:
                    return None
        else:
            ridx = self.reporter_ids
            cnames = i

        for c in cnames:
            if c in self.db.data_group_names:
                idx = self.db.data_group_names[c]
                didx.append(idx)

            elif c in self.db.feature_names:
                idx = self.db.feature_names[c]
                fidx.append(idx)
            else:
                raise Exception('Please provide a valid feature_name or data_group_name')

        if len(fidx) == 0:
            return self.db.data[ridx,:][:,didx]
        elif len(didx) == 0:
            return self.db.features[ridx,:][:,fidx]
        else:
            return np.hstack([self.db.data[ridx,didx],self.db.features[ridx,didx]])


                        

    
            


         