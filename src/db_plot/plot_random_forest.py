import os
import numpy as np


from src.db_analysis import model
from src.db_plot import plotter



def plot_predictions(db, fnum):
     
    for s in db.selectors:
        for i,m in enumerate(s.models):

            #plot predictions
            test_data = m.data[m.test_idx]
            test_features = m.features[m.test_idx, :]
            test_predictions = m.models.predict(test_features) 
            pred_fig, pred_ax = plotter.plot_scatter(test_data, test_predictions, xlabel = "Translation", ylabel = "Predicted Translation")
            #pred_outpath = os.path.join(db.output_path,f'{s.sample_id}_{s.sel_hash}_predictions.svg')
            #pred_fig.savefig(pred_outpath)

            #plot residuals
            residuals = np.abs(test_data - test_predictions)
            res_fig, res_ax = plotter.plot_scatter(test_data, residuals, xlabel = "Translation", ylabel = "Residuals")            
            #res_outpath = os.path.join(db.output_path,f'{s.sample_id}_{s.sel_hash}_residuals.svg')
            #res_fig.savefig(res_outpath)

            #plot feature importances
            feature_importances = m.feature_importances[0] 
            mean_fi = [f[1] for f in feature_importances][:fnum]
            std_fi = [f[2] for f in feature_importances][:fnum]
            fnames_fi = [f[0] for f in feature_importances][:fnum]
            pfi_fig, pfi_ax = plotter.plot_bar(xticks = fnames_fi, height = mean_fi, yerr = std_fi, ylabel = 'Feature Importance')
            #pfi_outpath = os.path.join(db.output_path,f'{s.sample_id}_{s.sel_hash}_feature_importances.svg')
            #pfi_fig.savefig(pfi_outpath)

            #plot feature correlations
            fc = db['feature_name','correlation'].where((db['reporter_group_id'] == s.selector_id) & (db['data_group_id'] == m.sample_id) & db['feature_name'].in_(fnames_fi)).to_list()
            fc = sorted(fc, key = lambda a:a[1], reverse = True)

            fn, corr = [],[]
            for f,c in fc:
                fn.append(f)
                corr.append(c)
            
            pfi_fig, pfi_ax = plotter.plot_bar(xticks = fn, height = corr, yerr = None, ylabel = 'Feature Correlation')
            #pfi_outpath = os.path.join(db.output_path,f'{m.sample_id}_{s.selector_id}_feature_importances.svg')




def run_models(db, params = {'corr_plus':0.25,'corr_minus':-0.25}):
    
    corr_plus = params.get('corr_plus')
    corr_minus = params.get('corr_minus')

    for s in db.selectors:
        s.models = []
        feature_names = db.select(['data_group_id','feature_name']).where(((db['correlation'] >= corr_plus) | (db['correlation'] <= corr_minus)) & (db['reporter_group_id'] == s.selector_id)).to_dict(group_by_key = True)
        for i,dgid in enumerate(s.data_group_ids):
            m = model.Model(s[:,feature_names[dgid]], feature_names[dgid], dgid, s.data[:,i], sel_hash = s.selector_id, model_ids = ['random_forest_regressor'], proc_num = 20, q = 0.3)
            m.train('random_forest_regressor', model_params = {'max_features' : 0.25, 'n_estimators': 200})
            m.permuted_feature_importances()
            s.models.append(m)
        plot_predictions(db, 12)
        break

