import numpy as np
import sklearn.ensemble
import sklearn.linear_model
import sklearn.model_selection
import sklearn.metrics
from sklearn.inspection import permutation_importance


model_parms = {
    'random_forest_regressor' : {'estimator' : sklearn.ensemble.RandomForestRegressor,
                                  'parameters' : {'bootstrap': [True],
                                                  'n_estimators': [20, 100, 200],
                                                  'max_features' : [0.1, 0.2, 0.3],
                                                  'max_depth' : [3, 5, 7],
                                                  'min_samples_split' : [2, 4, 8],
                                                  }
                                 },
    'elastic'                    : {'estimator' : sklearn.linear_model.Lasso,
                                  'parameters' : {'alpha': [0.05, 0.1, 0.5, 1.0]}
                                 },                              
}



class Model:

    def __init__(self, features, feature_names, sample_id, data, data_label, sel_hash, feature_correlations = None, model_ids = ['elastic'], proc_num = 10, q = 0.3):

        self.data_label = data_label    
        self.features = features
        self.feature_names = feature_names
        self.feature_correlations = feature_correlations
        self.sample_id = sample_id
        self.data = data 
        self.model_ids = model_ids
        self.q = q
        self.sel_hash = sel_hash
        self.random_state = np.random.randint(0,1000)

        self.proc_num = proc_num
        self.reporter_num = data.shape[0]
        self.feature_num = features.shape[1]

        self.training_idx = []
        self.test_idx = []
        self.models = []

        
        self.training_test()

        
    def training_test(self):
        self.training_idx, self.test_idx = sklearn.model_selection.train_test_split(range(self.reporter_num), test_size = self.q, random_state = self.random_state)

    def get_best_parameters(self):

        self.cross_validation = []

        for m in self.model_ids:
            estimator = model_parms[m]['estimator']
            params = model_parms[m]['parameters']
            cv = sklearn.model_selection.GridSearchCV(estimator(), params, cv = 5, n_jobs = self.proc_num, verbose = 1)
            cv.fit(self.features[self.training_idx, :], self.data[self.training_idx])
            self.cross_validation.append(cv)
            self.models.append(cv.best_estimator_)

    def train(self, model_id, model_params):
        
        estimator = model_parms[model_id]['estimator']
        m = estimator(**model_params)
        m.fit(self.features[self.training_idx, :], self.data[self.training_idx])
        self.models.append(m)
        
    def permuted_feature_importances(self):
        
        self.feature_importances = []

        for m in self.models:
            p = permutation_importance(m, self.features[self.test_idx, :], self.data[self.test_idx], n_repeats = 10, random_state = self.random_state, n_jobs = self.proc_num)
            pfi = [[fn, pmean, pstd] for fn, pmean, pstd in zip(self.feature_names, p.importances_mean, p.importances_std)]
            pfi = sorted(pfi, key = lambda x: x[1], reverse = True)
            self.feature_importances.append(pfi)
    

    def make_importance_logo(self):

        base_list = ['A', 'C', 'G', 'T']
        self.logos = []

        for pfi in self.feature_importances:
            logo = np.zeros((4, 10))
            for fn, pmean, pstd in pfi:
                b = fn.split('_')[0]
                
                if len(b) == 1:
                    i = int(fn.split('_')[1]) + 6
                    i = i - 1 if i > 0 else i 
                    logo[base_list.index(b),i] = pmean
                
                
            self.logos.append(logo)



    def feature_PCA(self, q):

        n_components = round(self.feature_num * q)
        pca = sklearn.decomposition.PCA(n_components = n_components)
        pca.fit(self.features)

        return pca
    

    def run_model(self, model_ids):

        for m in model_ids:
            if m in model_parms:
                estimator = model_parms[m]['estimator']
                params = model_parms[m]['parameters']
                search_results = sklearn.model_selection.GridSearchCV(estimator, params, cv = 5, n_jobs = -1, verbose = 1)

            else:
                raise Exception(f'Estimator {m} not found in models.')

        


def to_db(self, params):

    if 'model' not in self.tables:
        self.new_table('model', ['model_type', 'model_name','reporter_group_id','data_id','model'], [int, str], primary_key = 'model_id')


        
    