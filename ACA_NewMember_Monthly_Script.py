##import packages
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import joblib
import numpy as np
import pickle
from sklearn.preprocessing import MinMaxScaler
import pyodbc
import sys
import sqlalchemy
import urllib
import xgboost as xgb
from xgboost.sklearn import XGBClassifier
from catboost import Pool, CatBoostClassifier, CatBoostRegressor
import time
import gc
from future.standard_library import install_aliases
import datetime as dt
import shap


##define connection
def main():
    server = input("SQL Server: ")
    database = input("Database: ")
    year = input("Year: ")
    month = input("""Month(#) (1-11)
 (1-11 for that many months of claims data): """)
    sql = input("Run Data Inputs SQL Procedure (Y/N)?: ")
    viz = input("Run New Member Tableau SQL Procedure (Y/N)?: ")
    
    install_aliases()
    warnings.filterwarnings("ignore")
    cxn_str="DSN={0};DATABASE={1};Trusted_Connection=yes".format(server, database)                                   
    params = urllib.parse.quote_plus(cxn_str)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    

##check if proc exists and if so delete
    path = '/mnt/h/02_NoPHI/DataScience/01_ModelScripts/ACA_NewMember/create_DS_NewMember_inputs_monthly_production.sql'
    proc = open(path, encoding = 'utf-8')
    proc.read(1)
    code = proc.read()

    if not engine.execute("SELECT * FROM sys.objects WHERE type = 'P' and name = 'DS_ACA_NewMember_Inputs_monthly'").fetchall():
        engine.execute(code)
    else:
        engine.execute("DROP PROCEDURE DS_ACA_NewMember_Inputs_monthly")
        engine.execute(code)
    
    path = '/mnt/h/02_NoPHI/DataScience/01_ModelScripts/ACA_NewMember/create_DS_NewMember_monthly_ACA_postprocess.sql'
    proc = open(path, encoding = 'utf-8')
    proc.read(1)
    code = proc.read()

    if not engine.execute("SELECT * FROM sys.objects WHERE type = 'P' and name = 'DS_NewMember_ACA_Monthly_PostProcess'").fetchall():
        engine.execute(code)
    else:
        engine.execute("DROP PROCEDURE DS_NewMember_ACA_Monthly_PostProcess")
        engine.execute(code)
    
    print("**************************************************")
    print('procedures updated')
    
    
##pull tables with counts
    tables = ['DS_NewMember_001_ACA_Info','DS_NewMember_002_capturedconditions','DS_NewMember_003_nonscoredconditions','DS_NewMember_004_procedures','DS_NewMember_005_drugs','DS_NewMember_006_county_tpd']
    
    starttime = time.time()
    conn = engine.connect().connection
    cursor = conn.cursor()
    if sql == "Y" or sql == "y":
        cursor.execute("EXEC DS_ACA_NewMember_Inputs_monthly {}, {}".format(year,month))
    elif sql == "N" or sql == "n": 
        pass
    else: 
        sys.exit('''
    you killed pusheen! check your inputs and try again
───────────────────────────────────────
───▐▀▄───────▄▀▌───▄▄▄▄▄▄▄─────────────
───▌▒▒▀▄▄▄▄▄▀▒▒▐▄▀▀▒██▒██▒▀▀▄──────────
──▐▒▒▒▒▀▒▀▒▀▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▀▄────────
──▌▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▄▒▒▒▒▒▒▒▒▒▒▒▒▀▄──────
▀█▒▒▒XX▒▒█▒▒XX▒▒▒▀▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌─────
▀▌▒▒▒▒▒▒▀▒▀▒▒▒▒▒▒▀▀▒▒▒▒▒▒▒▒▒▒▒▒▒▒▐───▄▄
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌▄█▒█
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒█▒█▀─
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒█▀───
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌────
─▌▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▐─────
─▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌─────
──▌▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▐──────
──▐▄▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▄▌──────
────▀▄▄▀▀▀▀▀▄▄▀▀▀▀▀▀▀▄▄▀▀▀▀▀▄▄▀────────''')
    conn.commit()
    cursor.close()
    conn.close()
    print("**************************************************")
    print("SQL procedure finished in {} minutes".format(np.rint((time.time()-starttime)/60)))

    for i in range(0,len(tables)):
        rows = engine.execute('SELECT Count(*) FROM {}'.format(tables[i])).fetchone()[0]
        print("    {0} is {1} rows".format(tables[i], rows))
        if rows == 0:
            sys.exit('a sql input table is empty! please check that the required tables for the model are populated then rerun!')
        else: pass
    starttime = time.time()
    
    result = engine.execute("SELECT * FROM DS_NewMember_001_ACA_Info")
    df_mem = pd.DataFrame(result.fetchall())
    df_mem.columns = result.keys()
    
    result = engine.execute("SELECT * FROM DS_NewMember_002_capturedconditions")
    df_cc = pd.DataFrame(result.fetchall())
    df_cc.columns = result.keys()

    result = engine.execute("SELECT * FROM DS_NewMember_003_nonscoredconditions")
    df_noncc = pd.DataFrame(result.fetchall())
    df_noncc.columns = result.keys()
    
    result = engine.execute("SELECT * FROM DS_NewMember_004_procedures")
    df_procs = pd.DataFrame(result.fetchall())
    df_procs.columns = result.keys()
    
    result = engine.execute("SELECT * FROM DS_NewMember_005_drugs")
    df_drugs = pd.DataFrame(result.fetchall())
    df_drugs.columns = result.keys()

    result = engine.execute("SELECT * FROM DS_NewMember_006_county_tpd")
    df_tpd = pd.DataFrame(result.fetchall())
    df_tpd.columns = result.keys()
    
    
##etl data
    df_cc_pivot = pd.pivot_table(df_cc, values='instances', index=['clientDSMemberKey','active_year'], columns=['CC']).add_suffix('_captured')
    df_noncc_pivot = pd.pivot_table(df_noncc, values='instances', index=['clientDSMemberKey','active_year'], columns=['CC']).add_suffix('_noncaptured')
    df_drugs['drug_subclass'] = pd.to_numeric(df_drugs.drug_subclass)
    df_drugs100 = df_drugs[['drug_subclass','instances']].groupby('drug_subclass').sum().sort_values('instances',ascending=False)[0:100].reset_index()
    df_drugs=df_drugs.merge(df_drugs100, how='inner', on='drug_subclass').drop('instances_y',axis=1)
    df_drugs_pivot = pd.pivot_table(df_drugs, values='instances_x', index=['clientDSMemberKey','active_year'],columns=['drug_subclass'])
    df_drugs_pivot.columns = df_drugs_pivot.columns.astype(str)
    df_procs['description'] = df_procs['category'] + "_" + df_procs['class']
    df_procs_pivot = pd.pivot_table(df_procs, values='instances', index=['clientDSMemberKey','active_year'],columns=['description'])
    df_tpd_pivot = pd.pivot_table(df_tpd, values='value', index=['clientDSMemberKey'],columns=['metric'])
    df_noncc_pivot = df_noncc_pivot.reset_index()
    df_cc_pivot = df_cc_pivot.reset_index()
    df_drugs_pivot = df_drugs_pivot.reset_index()
    df_procs_pivot = df_procs_pivot.reset_index()
    df_tpd_pivot = df_tpd_pivot.reset_index() 
    
    df_final = pd.merge(df_mem, df_noncc_pivot, on=['clientDSMemberKey','active_year'],how='left')
    df_final = pd.merge(df_final, df_cc_pivot, on=['clientDSMemberKey','active_year'],how='left')
    df_final = pd.merge(df_final, df_drugs_pivot, on=['clientDSMemberKey','active_year'],how='left')
    df_final = pd.merge(df_final, df_procs_pivot, on=['clientDSMemberKey','active_year'],how='left')
    df_final = pd.merge(df_final, df_tpd_pivot, on=['clientDSMemberKey'],how='left')
    df_final['sex'] = df_final['sex'].map({'M':1, 'F':0})
    df_final['relationship'] = df_final['relationship'].map({'':0})
    df_final['metalLevel']=df_final['metalLevel'].astype(int)
    df_final['medClaimTotal_x']=df_final['medClaimTotal']
    df_final['rxClaimTotal_x']=df_final['rxClaimTotal']
    df_final['medClaimTotal_x']=df_final['medClaimTotal_x'].astype(int)
    df_final['rxClaimTotal_x']=df_final['rxClaimTotal_x'].astype(int)
    df_final.fillna(0, inplace = True)
    df_final = df_final.replace([np.inf,-np.inf],0) 
    for col in df_final.select_dtypes(include=['float64']).columns:
        df_final[col] = df_final[col].astype('int64')
    
    del(df_mem)
    del(df_cc)
    del(df_noncc)
    del(df_procs)
    del(df_drugs)
    del(df_tpd)
    del(df_cc_pivot)
    del(df_noncc_pivot)
    del(df_procs_pivot)
    del(df_drugs_pivot)
    del(df_tpd_pivot)
    gc.collect()


##run models
    print("**************************************************")
    print('data imported and ETLd')
    print('beginning modeling...')

##run xgb model   
##drop model
    print('    '+'drop')
    drop = xgb.XGBClassifier()
    drop.load_model('/mnt/ds/notebooks/NathanStevenson/New_Member_ACA_Monthly/Pickles/xgboost_ACA_NewMember_drop_month{}.model'.format(month))
    #load features
    df_features = pd.read_csv('/mnt/ds/notebooks/NathanStevenson/New_Member_ACA_Monthly/Pickles/features_model{}.csv'.format(month))
    features = df_features['features'].to_list()
    missing = set(features)-set(df_final.columns.tolist())
    for col in missing:
        df_final[col]=0    
    df_final.fillna(0, inplace = True)
    df_final_drop = df_final
    df_final_drop = df_final.reindex(columns=drop.get_booster().feature_names)
    df_final['drop'] = drop.predict_proba(df_final_drop[features])[:,1]
    
##run catboost models
##cost model
    print('    '+'sumcost')
    sumcost = joblib.load('/mnt/ds/notebooks/NathanStevenson/New_Member_ACA_Monthly/Pickles/catboost_ACA_NewMember_cost_month{}.pkl'.format(month))
    missing = set(sumcost.feature_names_)-set(df_final.columns.tolist())
    for col in missing:
        df_final[col]=0
    df_final.fillna(0, inplace = True)
    df_final_sumcost = df_final
    df_final_sumcost = df_final.reindex(columns=sumcost.feature_names_)
    for col in df_final_sumcost[sumcost.feature_names_].select_dtypes(include=['float64']).columns:
        df_final_sumcost[col] = df_final_sumcost[col].astype('int64')
    y_pred_cost=sumcost.predict(df_final_sumcost[sumcost.feature_names_])
    df_final['y_pred_cost']=y_pred_cost
    df_final['y_pred_cost'] = df_final['y_pred_cost'].replace({'high':0,'low':1,'med':2})
    df_final['high_cost'] = sumcost.predict_proba(df_final_sumcost[sumcost.feature_names_])[:,0]
    df_final['low_cost'] = sumcost.predict_proba(df_final_sumcost[sumcost.feature_names_])[:,1]
    df_final['med_cost'] = sumcost.predict_proba(df_final_sumcost[sumcost.feature_names_])[:,2]
    #load shapley values
    explainerXGB = shap.TreeExplainer(sumcost)
    shap_values_all=explainerXGB.shap_values(df_final[features])
    #filter to only med and high category for shap
    df_current=df_final[df_final['y_pred_cost']!=1].copy()
    df_current.drop_duplicates(inplace=True)
    for idx, row in df_current.iterrows():
        row2=df_final.iloc[[idx]]
        which_class = df_current.at[idx,'y_pred_cost']
        shap_values = explainerXGB.shap_values(row2[features]) 
        df=pd.DataFrame(data=shap_values[which_class], columns=df_current[features].columns, index=[0])            
        df1 = df.transpose()
        df1.sort_values(by=0, ascending=True, inplace = True)
        for i in range(0,5):
            feature = df1.index.values[i]        
            value=df_current.at[idx,feature]
            if pd.isnull(value):
                value='unknown'
            feat_val=str(str(feature) + " = " + str(value))
            feat_insert="Feature_{0}_Cost".format(i+1)
            df_current.at[idx,feat_insert]=feat_val
    df_final = pd.merge(df_final, df_current[['clientDSMemberKey', 'Feature_1_Cost', 'Feature_2_Cost', 'Feature_3_Cost', 'Feature_4_Cost', 'Feature_5_Cost']], on=['clientDSMemberKey'],how='left')
    df_final.fillna('', inplace = True)
    
##risk model
    print('    '+'totalriskscore')
    risk = joblib.load('/mnt/ds/notebooks/NathanStevenson/New_Member_ACA_Monthly/Pickles/catboost_ACA_NewMember_risk_month{}.pkl'.format(month))
    missing = set(risk.feature_names_)-set(df_final.columns.tolist())
    for col in missing:
        df_final[col]=0
    df_final.fillna(0, inplace = True)
    df_final_risk = df_final
    df_final_risk = df_final.reindex(columns=risk.feature_names_)
    for col in df_final_risk[risk.feature_names_].select_dtypes(include=['float64']).columns:
        df_final_risk[col] = df_final_risk[col].astype('int64')
    y_pred_risk=risk.predict(df_final_risk[risk.feature_names_])
    df_final['y_pred_risk']=y_pred_risk
    df_final['y_pred_risk'] = df_final['y_pred_risk'].replace({'high':0,'low':1,'med':2})
    df_final['high_risk'] = risk.predict_proba(df_final_risk[risk.feature_names_])[:,0]
    df_final['low_risk'] = risk.predict_proba(df_final_risk[risk.feature_names_])[:,1]
    df_final['med_risk'] = risk.predict_proba(df_final_risk[risk.feature_names_])[:,2]
    #filter to only med and high category for shap
    df_current=df_final[df_final['y_pred_risk']!=1].copy()
    df_current.drop_duplicates(inplace=True)
    df_current.reset_index(drop=True, inplace=True)
    #load shapley values
    explainerXGB = shap.TreeExplainer(risk)
    shap_values_all=explainerXGB.shap_values(df_final[features]) 
    for idx, row in df_current.iterrows():
        row2=df_final.iloc[[idx]]
        which_class = df_current.at[idx,'y_pred_risk']
        shap_values = explainerXGB.shap_values(row2[features]) 
        df=pd.DataFrame(data=shap_values[which_class], columns=df_current[features].columns, index=[0])            
        df1 = df.transpose()
        df1.sort_values(by=0, ascending=True, inplace = True)
        for i in range(0,5):
            feature = df1.index.values[i]        
            value=df_current.at[idx,feature]
            if pd.isnull(value):
                value='unknown'
            feat_val=str(str(feature) + " = " + str(value))
            feat_insert="Feature_{0}_Risk".format(i+1)
            df_current.at[idx,feat_insert]=feat_val
    df_final = pd.merge(df_final, df_current[['clientDSMemberKey','Feature_1_Risk', 'Feature_2_Risk', 'Feature_3_Risk', 'Feature_4_Risk', 'Feature_5_Risk']], on=['clientDSMemberKey'],how='left')
    df_final.fillna('', inplace = True)
    
##final cleanup
    gc.collect()
    df_final.drop_duplicates(subset='clientDSMemberKey', keep='first', inplace=True)
    df_final['Month'] = month
    df_final['runDate'] = dt.datetime.today().strftime('%Y-%m-%d')
    features_predict = ['clientDSMemberKey', 'drop', 'high_cost', 'low_cost', 'med_cost', 'Feature_1_Cost', 'Feature_2_Cost', 'Feature_3_Cost', 'Feature_4_Cost', 'Feature_5_Cost', 'high_risk', 'low_risk', 'med_risk', 'Month', 'runDate', 'Feature_1_Risk', 'Feature_2_Risk', 'Feature_3_Risk', 'Feature_4_Risk', 'Feature_5_Risk']
    print('modeling complete')
    print("**************************************************")

    
##check for same day run and delete old data
    conn = engine.connect().connection
    cursor = conn.cursor()
    result = engine.execute("SELECT MAX(rundate) as rundate FROM DS_Clone_007_output WHERE month = '{}'".format(month))
    rundate = pd.DataFrame(result.fetchall(),columns=result.keys())
    now = dt.datetime.now().strftime('%Y-%m-%d')
    if rundate['rundate'].iloc[0] == None:
        pass
    else:
        if rundate['rundate'].iloc[0].strftime('%Y-%m-%d') == dt.datetime.now().strftime('%Y-%m-%d'):
            engine.execute("DELETE FROM DS_Clone_007_output WHERE month = '{0}' AND runDate='{1}'".format(month,now))
            engine.execute("DELETE FROM DS_NewMember_004_LabeledOutput WHERE month = '{0}' AND runDate='{1}'".format(month,now))
            print('deleted old results from today!')
        else:
            print('no run yet today!')
    cursor.close()
    conn.close()
    conn = engine.connect().connection
    cursor = conn.cursor()

    
##insert outputs into sql
##sql insert
    dft = [str(tuple(x)) for x in df_final[features_predict].values]
    denom =  len(df_final[features_predict])
    num = 0
    counter = 0
    insert_ = """

        INSERT INTO DS_Clone_007_output 
        (clientDSMemberKey, DropProbability, High_Cost, Low_Cost, Med_Cost, Feature_1_Cost, Feature_2_Cost, Feature_3_Cost, Feature_4_Cost, Feature_5_Cost, High_Risk, Low_Risk, Med_Risk, month, runDate, Feature_1_Risk, Feature_2_Risk, Feature_3_Risk, Feature_4_Risk, Feature_5_Risk)
        VALUES

        """;

    def tickertron(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    for batch in tickertron(dft, 1000):
        rows = ','.join(batch)
        insert_rows = insert_ + rows
        cursor.execute(insert_rows)
        conn.commit()
        counter = counter + 1
        num = num + 1000
        if counter > 99:
            print(float(num) / float(denom))
            counter = 0
    cursor.close()
    conn.close()
    
    ##final output tables counts
    tables = ['DS_Clone_007_output','DS_NewMember_004_LabeledOutput']
    conn = engine.connect().connection
    cursor = conn.cursor()
    cursor.execute("EXEC DS_NewMember_ACA_Monthly_PostProcess {}".format(month))
    conn.commit()
    cursor.close()
    conn.close()   
    for i in range(0,len(tables)):
        rows = engine.execute('SELECT Count(*) FROM {}'.format(tables[i])).fetchone()[0]
        print("    {0} is {1} rows".format(tables[i], rows))
    
    print('results have been input into SQL!')
    print("**************************************************")
    
    
    ##run tableau viz
    path = '/mnt/h/02_NoPHI/DataScience/01_ModelScripts/ACA_NewMember/Standard_New_Member_Tableau_Table_Setup_Monthly.sql'
    proc = open(path, encoding = 'utf-8')
    proc.read(1)
    code = proc.read()
    if not engine.execute("SELECT * FROM sys.objects WHERE type = 'P' and name = 'Viz_DS_ACA_NewMember_Inputs_monthly'").fetchall():
        engine.execute(code)
    else:
        engine.execute("DROP PROCEDURE Viz_DS_ACA_NewMember_Inputs_monthly")
        engine.execute(code)
    starttime = time.time()
    conn = engine.connect().connection
    cursor = conn.cursor()
    if viz == "Y" or viz == "y":
        print("running viz SQL proc...")  
        cursor.execute("EXEC Viz_DS_ACA_NewMember_Inputs_monthly {}".format(month))
    elif viz == "N" or viz == "n": 
        pass
    else: 
        sys.exit('''
    you killed pusheen! check your inputs and try again
───────────────────────────────────────
───▐▀▄───────▄▀▌───▄▄▄▄▄▄▄─────────────
───▌▒▒▀▄▄▄▄▄▀▒▒▐▄▀▀▒██▒██▒▀▀▄──────────
──▐▒▒▒▒▀▒▀▒▀▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▀▄────────
──▌▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▄▒▒▒▒▒▒▒▒▒▒▒▒▀▄──────
▀█▒▒▒XX▒▒█▒▒XX▒▒▒▀▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌─────
▀▌▒▒▒▒▒▒▀▒▀▒▒▒▒▒▒▀▀▒▒▒▒▒▒▒▒▒▒▒▒▒▒▐───▄▄
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌▄█▒█
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒█▒█▀─
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒█▀───
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌────
─▌▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▐─────
─▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌─────
──▌▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▐──────
──▐▄▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▄▌──────
────▀▄▄▀▀▀▀▀▄▄▀▀▀▀▀▀▀▄▄▀▀▀▀▀▄▄▀────────''')
    conn.commit()
    cursor.close()
    conn.close()
    print("**************************************************")
    print("viz SQL procedure finished in {} minutes".format(np.rint((time.time()-starttime)/60)))    

    
    print("**************************************************")
    print("**************************************************")
    print('done! good job! purrr-fect!')
    print("**************************************************")
    print("**************************************************")
    print('''───────────────────────────────────────
───▐▀▄───────▄▀▌───▄▄▄▄▄▄▄─────────────
───▌▒▒▀▄▄▄▄▄▀▒▒▐▄▀▀▒██▒██▒▀▀▄──────────
──▐▒▒▒▒▀▒▀▒▀▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▀▄────────
──▌▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▄▒▒▒▒▒▒▒▒▒▒▒▒▀▄──────
▀█▒▒▒█▌▒▒█▒▒▐█▒▒▒▀▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌─────
▀▌▒▒▒▒▒▒▀▒▀▒▒▒▒▒▒▀▀▒▒▒▒▒▒▒▒▒▒▒▒▒▒▐───▄▄
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌▄█▒█
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒█▒█▀─
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒█▀───
▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌────
─▌▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▐─────
─▐▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▌─────
──▌▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▐──────
──▐▄▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▄▌──────
────▀▄▄▀▀▀▀▀▄▄▀▀▀▀▀▀▀▄▄▀▀▀▀▀▄▄▀────────''')
    
    
if __name__ == "__main__":
    main()