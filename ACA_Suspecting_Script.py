import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import joblib
import numpy as np
import random
import pickle
from sklearn.linear_model import LogisticRegression
from sklearn import linear_model, model_selection, ensemble, svm, naive_bayes, preprocessing
from sklearn.metrics import classification_report, confusion_matrix, precision_score, precision_recall_fscore_support, accuracy_score, balanced_accuracy_score, roc_auc_score, r2_score, mean_absolute_error, mean_squared_error
from sklearn.decomposition import FactorAnalysis
from sklearn.naive_bayes import MultinomialNB, BernoulliNB, GaussianNB
from sklearn.model_selection import RandomizedSearchCV, GridSearchCV
import getpass
import pyodbc
import sys
import sqlalchemy
import urllib
from datetime import date
from collections import Counter
import xgboost as xgb
from xgboost.sklearn import XGBClassifier, XGBRegressor
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 12, 4
from sklearn.decomposition import TruncatedSVD, PCA
from sklearn.preprocessing import MinMaxScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neighbors import KNeighborsRegressor
import time
from catboost import Pool, CatBoostClassifier, CatBoostRegressor
import datetime as dt
import math
from sklearn.metrics import precision_score, precision_recall_fscore_support, accuracy_score, balanced_accuracy_score
from autogluon.tabular import TabularDataset, TabularPredictor


##define connection
def main():
    ##define connection
    server = input("SQL Server: ")
    database = input("Database: ")
    year = input("Year: ")
    c000_proc=input("Run C_000_B4_ExecuteBig4? (Y/N): ")
    if((c000_proc=='y')|(c000_proc=='Y')):
        c000_proc=True
    elif((c000_proc=='n')|(c000_proc=='N')):
        c000_proc=False
    else:
        raise Exception('ERROR: Invalid input for Big Four parameter')
    e000_proc=input("Run E_000_Create_DataMart_Tables? (Y/N): ")
    if((e000_proc=='y')|(e000_proc=='Y')):
        e000_proc=True
    elif((e000_proc=='n')|(e000_proc=='N')):
        e000_proc=False
    else:
        raise Exception('ERROR: Invalid input for Data Mart parameter')
    g000_proc=input("Run G_000_Create_Output_Tables? (Y/N): ")
    if((g000_proc=='y')|(g000_proc=='Y')):
        g000_proc=True
    elif((g000_proc=='n')|(g000_proc=='N')):
        g000_proc=False
    else:
        raise Exception('ERROR: Invalid input for Output Tables parameter')
    warnings.filterwarnings("ignore")
    cxn_str="DSN={0};DATABASE={1};Trusted_Connection=yes".format(server, database)                                   
    params = urllib.parse.quote_plus(cxn_str)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params),isolation_level="AUTOCOMMIT") 
    print("**************************************************")
    print("Pulling in Suspecting Input Tables...")

    
##pull tables with counts
    tables = ['DS_Suspecting_001_Info', 'DS_Suspecting_002_Conditions', 'DS_Suspecting_003_Persistent', 'DS_Suspecting_004_Drugs', 
              'DS_Suspecting_005_Procedures', 'DS_Suspecting_006_county_tpd']

    for i in range(0,len(tables)):
        rows = engine.execute('SELECT Count(*) FROM {}'.format(tables[i])).fetchone()[0]
        print("    {0} is {1} rows".format(tables[i], rows))
        if rows == 0:
            sys.exit('a sql input table is empty! please check that the required tables for the model are populated then rerun!')
        else: pass

##pull in data
    b=database
    starttime = time.time() 
    result = engine.execute('SELECT DISTINCT * FROM DS_Suspecting_001_Info')
    df_mem = pd.DataFrame(result.fetchall(),columns=result.keys())

    result = engine.execute('SELECT DISTINCT * FROM DS_Suspecting_002_Conditions')
    df_cc = pd.DataFrame(result.fetchall(),columns=result.keys())
    df_cc['clientDB'] = b

    result = engine.execute('SELECT DISTINCT * FROM DS_Suspecting_002_Conditions')
    df_cc = pd.DataFrame(result.fetchall(),columns=result.keys())
    df_cc['clientDB'] = df_cc['clientDB'].fillna(b)

    result = engine.execute('SELECT DISTINCT * FROM DS_Suspecting_003_Persistent')
    df_pers = pd.DataFrame(result.fetchall(),columns=result.keys())
    df_pers['clientDB'] = b

    result = engine.execute('SELECT DISTINCT * FROM DS_Suspecting_005_Procedures')
    df_procs = pd.DataFrame(result.fetchall(),columns=result.keys())
    df_procs['clientDB'] = df_procs['clientDB'].fillna(b)

    result = engine.execute('SELECT DISTINCT * FROM DS_Suspecting_004_Drugs')
    df_drugs = pd.DataFrame(result.fetchall(),columns=result.keys())
    df_drugs['clientDB'] = b

    result = engine.execute('SELECT DISTINCT * FROM DS_Suspecting_006_county_tpd')
    df_tpd = pd.DataFrame(result.fetchall(),columns=result.keys())
    df_tpd['clientDB'] = df_tpd['clientDB'].fillna(b)
    print('Data Loaded')

    top_drug_ccs=pd.read_csv('/mnt/ds/notebooks/AliciaErwin/Suspecting_ACA/top_drug_ccs4.csv')
    df_drug_cols=pd.read_csv('/mnt/ds/notebooks/AliciaErwin/Suspecting_ACA/drug_cols.csv')
    model_metrics_newyear=pd.read_csv('/mnt/ds/notebooks/AliciaErwin/Suspecting_ACA/model_metrics_newyear.csv')
    
    
##etl data
    df_cc['instances'] = df_cc['instances'].astype(float)
    df_pers[['instances_y1','instances_y2','instances_y3','dayssincecoded']] = df_pers[['instances_y1','instances_y2','instances_y3','dayssincecoded']].astype(float)
    df_drugs[['rx_fill_count_y0','rx_fill_count_y1','dayssincefilled']] = df_drugs[['rx_fill_count_y0','rx_fill_count_y1','dayssincefilled']].astype(float)
    df_procs[['instances_y0','instances_y1']] = df_procs[['instances_y0','instances_y1']].astype(float)
    df_tpd['value'] = df_tpd['value'].astype(float)

    ##convert HIPPA codes to the standard CWF codes
    relationship_set = df_mem['relationship'].unique().tolist()
    if ('18' in relationship_set) & ('19' in relationship_set):
        df_mem['relationship'] = df_mem['relationship'].map({'19':'3','18':'1','21':'9','01':'2'})
    
    df_cc['cc_instances_y0']=df_cc['cc'].astype(str)+'_'+df_cc['scored'].astype(str)
    df_cc_pivot = pd.pivot_table(df_cc, values='instances', index=['clientDSMemberKey','clientDB'], columns=['cc_instances_y0'])

    df_pers_pivot = pd.pivot_table(df_pers, values=['instances_y1','instances_y2','instances_y3','dayssincecoded'], index=['clientDSMemberKey','clientDB'], columns=['cc'])
    df_pers_pivot.columns = df_pers_pivot.columns.to_series().str.join('_')

    df_drugs['drug_subclass'] = pd.to_numeric(df_drugs.drug_subclass)
    df_drugs_pivot = pd.pivot_table(df_drugs, values=['rx_fill_count_y0','rx_fill_count_y1','dayssincefilled'], index=['clientDSMemberKey','clientDB'],columns=['drug_subclass'])
    df_drugs_pivot.columns = [col[0]+f"_{col[1]}" for col in df_drugs_pivot.columns]

    #using top 100 procedure codes - filter out a few in a later step
    df_procs100_y1 = df_procs[['category','instances_y1']].groupby('category').sum().sort_values('instances_y1',ascending=False)[0:100].reset_index()
    df_procs=df_procs.merge(df_procs100_y1, how='inner', on='category').drop('instances_y1_y',axis=1)
    df_procs = df_procs.rename(columns={"instances_y1_x":"instances_y1"})
    df_procs_pivot = pd.pivot_table(df_procs, values=['instances_y0','instances_y1'], index=['clientDSMemberKey','clientDB',],columns=['category'])
    df_procs_pivot.columns = [col[0]+f"_{col[1]}" for col in df_procs_pivot.columns]

    df_tpd_pivot = pd.pivot_table(df_tpd, values='value', index=['clientDSMemberKey','clientDB'],columns=['metric'])

    df_cc_pivot = df_cc_pivot.reset_index()
    df_pers_pivot = df_pers_pivot.reset_index()
    df_drugs_pivot = df_drugs_pivot.reset_index()
    df_procs_pivot = df_procs_pivot.reset_index()
    df_tpd_pivot = df_tpd_pivot.reset_index()

    df_procs100_y1 = df_procs[['category','instances_y1']].groupby('category').sum().sort_values('instances_y1',ascending=False)[0:100].reset_index()
    df_procs=df_procs.merge(df_procs100_y1, how='inner', on='category').drop('instances_y1_y',axis=1)
    df_procs = df_procs.rename(columns={"instances_y1_x":"instances_y1"})
    df_procs_pivot = pd.pivot_table(df_procs, values=['instances_y0','instances_y1'], index=['clientDSMemberKey','clientDB',],columns=['category'])
    df_procs_pivot.columns = [col[0]+f"_{col[1]}" for col in df_procs_pivot.columns]
    df_procs_pivot = df_procs_pivot.reset_index()

    df_final = pd.merge(df_mem, df_pers_pivot, on=['clientDSMemberKey','clientDB'],how='left')
    df_final = pd.merge(df_final, df_cc_pivot, on=['clientDSMemberKey','clientDB'],how='left')
    df_final = pd.merge(df_final, df_drugs_pivot, on=['clientDSMemberKey','clientDB'],how='left')
    df_final = pd.merge(df_final, df_procs_pivot, on=['clientDSMemberKey','clientDB'],how='left')
    df_final = pd.merge(df_final, df_tpd_pivot, on=['clientDSMemberKey','clientDB'],how='left')

    df_cc = pd.DataFrame()
    df_pers = pd.DataFrame()
    df_drugs = pd.DataFrame()
    df_tpd = pd.DataFrame()
    df_procs = pd.DataFrame()

    df_final['medClaimTotal_y1'] = df_final['medClaimTotal_y1'].astype(float)
    df_final['medClaimTotal_y0'] = df_final['medClaimTotal_y0'].astype(float)
    df_final['rxClaimTotal_y1'] = df_final['rxClaimTotal_y1'].astype(float)
    df_final['rxClaimTotal_y0'] = df_final['rxClaimTotal_y0'].astype(float)
    df_final['metalLevel'] = df_final['metalLevel'].astype(float)
    df_final['Spec_Coinsurance'] = df_final['Spec_Coinsurance'].astype(float)
    df_final['ER_Copay'] = df_final['ER_Copay'].astype(float)
    df_final['Drug_Deductible'] = df_final['Drug_Deductible'].astype(float)
    df_final['Med_MOOP'] = df_final['Med_MOOP'].astype(float)
    df_final['Drug_MOOP'] = df_final['Drug_MOOP'].astype(float)
    df_final['PCP_Coinsurance'] = df_final['PCP_Coinsurance'].astype(float)
    df_final['OP_Copay'] = df_final['OP_Copay'].astype(float)
    df_final['OP_Coinsurance'] = df_final['OP_Coinsurance'].astype(float)
    df_final['ER_Coinsurance'] = df_final['ER_Coinsurance'].astype(float)
    df_final['Med_Deductible'] = df_final['Med_Deductible'].astype(float)
    df_final['IP_Copay'] = df_final['IP_Copay'].astype(float)
    df_final['UrgentCare_Coinsurance'] = df_final['UrgentCare_Coinsurance'].astype(float)
    df_final['PCP_Copay'] = df_final['PCP_Copay'].astype(float)
    df_final['IP_Coinsurance'] = df_final['IP_Coinsurance'].astype(float)
    df_final['UrgentCare_Copay'] = df_final['UrgentCare_Copay'].astype(float)
    df_final['mm_y1'] = df_final['mm_y1'].astype(float)
    df_final['pro_claim_count_y1'] = df_final['pro_claim_count_y1'].astype(float)
    df_final['mm_y2'] = df_final['mm_y2'].astype(float)
    df_final['op_claim_count_y1'] = df_final['op_claim_count_y1'].astype(float)
    df_final['Spec_Copay'] = df_final['Spec_Copay'].astype(float)
    df_final['PCP_Copay']=df_final['PCP_Copay'].astype(float)
    df_final['ip_claim_count_y1'] = df_final['ip_claim_count_y1'].astype(float)
    df_final['CSR_Indicator'] = df_final['CSR_Indicator'].astype(float)
    df_final['mm_y3'] = df_final['mm_y3'].astype(float)
    df_final['age'] = df_final['age'].astype(float)
    df_final['sex'] = df_final['sex'].map({'M':1, 'F':0})  
    df_final['relationship'] = df_final['relationship'].map({'SELF':'1', 'Self':'1', 'SPOUSE':'2', 'DEPENDENT':'3', '':'9', ' ':'9','LIFE PART':'2'})
    df_final['relationship'] = df_final['relationship'].astype(float)
    
    finalpath=['102_1', '103_1', '107_1', '108_1', '109_1', '10_1', '111_1', '112_1', '113_1', '114_1', '115_1', '117_1', '118_1', '119_1', '121_1', '123_1', '125_1', '126_1',
 '128_1', '129_1', '12_1', '131_1', '132_1', '135_1', '137_1', '138_1', '13_1', '142_1', '145_1', '146_1', '149_1', '150_1', '151_1', '153_1', '154_1',
 '158_1', '159_1', '162_1', '163_1', '174_1', '183_1', '184_1', '188_1', '18_1', '19_1', '203_1', '204_1', '207_1', '209_1', '210_1', '211_1',
 '212_1', '217_1', '219_1', '223_1', '226_1', '228_1', '234_1', '242_1', '243_1', '244_1', '245_1', '247_1', '249_1', '251_1', '253_1', '254_1',
 '27_1', '28_1', '29_1', '30_1', '34_1', '35_1_1', '35_2_1', '36_1', '37_1_1', '37_2_1', '3_1', '45_1', '46_1', '47_1', '48_1', '4_1', '54_1', '55_1', '61_1',
 '63_1', '64_1', '66_1', '67_1', '68_1', '69_1', '70_1', '71_1', '75_1', '81_1', '83_1', '87_1_1', '8_1', '94_1', '96_1', '9_1']
    
    autopath=['110_1', '11_1', '120_1', '122_1', '127_1', '130_1', '139_1', '156_1', '160_1', '161_1_1', '161_2_1', '187_1', '1_1', '205_1',
 '208_1', '20_1', '21_1', '23_1', '246_1', '248_1', '2_1', '42_1', '45_1', '46_1', '47_1', '48_1', '54_1', '56_1', '57_1', '62_1',
 '6_1', '74_1', '82_1', '84_1', '87_2_1', '88_1', '90_1', '97_1']
    
    print('Beginning Modeling...')
##begin modeling
    ##add missing cols
    df_final_colcheck=pd.read_csv('/mnt/ds/notebooks/AliciaErwin/Suspecting_ACA/SuspACA_cols.csv')
    colcheck_features=[s for s in df_final_colcheck.columns[2:]]
    for feat in colcheck_features:
        if feat not in df_final.columns:
            df_final[feat]=np.nan
    features=list(set(df_final_colcheck[2:].columns.tolist()) - set(['clientDSMemberKey','clientDB','active_year','Drug_MOOP', 'Drug_Deductible', 
                                                      'ER_Copay', 'Med_Deductible', 'Med_MOOP', 'OP_Copay', 'pro_claim_count_y0',
                                                       'op_claim_count_y0', 'ip_claim_count_y0', 'mm_y0', 'instances_y0_Patient Evaluation and Management - Outpatient',
                                                      'Unnamed: 0','IP_Copay','GCF','Unnamed: 0.1'
                                                      ]))
    drug_features_all=df_drug_cols.columns[3:]
    top_models_ae=pd.read_csv('/mnt/ds/notebooks/AliciaErwin/Suspecting_ACA/top_models_ae.csv')  
    matching = [s for s in features if "fill" in s] 
    for feature in matching:
        if feature in features:
            #print(feature)
            features.remove(feature)        
    df_predict=pd.DataFrame()
    
    ##predict here
    for y in finalpath+autopath:
        df_final['event'] = df_final[y].apply(lambda x: 1 if x > 0 else 0)
        features_current=features.copy()
        features_current.remove(y)
        row2=top_drug_ccs[top_drug_ccs['cc']==y]
        drug_array=row2[['drug_1','drug_2','drug_3','drug_4','drug_5','drug_6','drug_7','drug_8', 'drug_9','drug_10']].values
        drug_array=drug_array[0]
        for d in drug_array:
            #print(d)
            result= [v for v in drug_features_all if str(d) in v]
            features_current.extend(result)
        print("    {0}".format(y))
        #print(df_final['event'].value_counts())

        positives=None
        if not df_final['event'].value_counts().get(1):
            positives=0
        else:
            positives=df_final['event'].value_counts()[1]

        features_current=list(set(features_current))
        if y in finalpath:
            #best_model_ae=top_models_ae[(top_models_ae['cc']==y)]['model'].values[0]
            hdrive='/mnt/h/02_NoPHI/DataScience/Alicia/ACA_Suspecting/autogluon/final_models/' + str(y) +'/'
            predictor=TabularPredictor.load(hdrive)


            test_metrics=None

    #        this commented out section would add overall model stats per condition for a db
    #         test_metrics=predictor.evaluate_predictions(y_true=df_final['event'], y_pred=y_pred, auxiliary_metrics=True)
    #         display(test_metrics)


    #         model_metrics_newyear=model_metrics_newyear.append({'database':test_db,
    #                                                             'year':test_year,
    #                                                 'cc':y,
    #                                                 'model':predictor.get_model_best(),
    #                                                 'f1':test_metrics["f1"],
    #                                                   'accuracy':test_metrics["accuracy"],
    #                                                   'balanced_accuracy':test_metrics["balanced_accuracy"],
    #                                                   'precision':test_metrics["precision"],
    #                                                   'recall':test_metrics["recall"],
    #                                                 'positives':positives},
    #                                  ignore_index=True)

            df_predict2=pd.DataFrame({'memberKey':df_final['clientDSMemberKey']})
            df_predict2['HCC']=y
            df_predict2['probability']=predictor.predict_proba(df_final[features_current])[1]
            df_predict=df_predict.append(df_predict2)
        elif y in autopath:      
            hdrive='/mnt/h/02_NoPHI/DataScience/Alicia/ACA_Suspecting/autogluon/' + str(y) +'/'
            predictor=TabularPredictor.load(hdrive)


            test_metrics=None
            modelchoice=top_models_ae[(top_models_ae['cc']==y)]['model'].values[0]
            y_pred=predictor.predict(df_final[features_current], model=modelchoice)


    #        this commented out section would add overall model stats per condition for a db   
    #         test_metrics=predictor.evaluate_predictions(y_true=df_final['event'], y_pred=y_pred, auxiliary_metrics=True)
    #         display(test_metrics)

    #        model_metrics_newyear=model_metrics_newyear.append({'database':test_db,
    #                                                             'year':test_year,
    #                                                 'cc':y,
    #                                                 'model':modelchoice,
    #                                                 'f1':test_metrics["f1"],
    #                                                   'accuracy':test_metrics["accuracy"],
    #                                                   'balanced_accuracy':test_metrics["balanced_accuracy"],
    #                                                   'precision':test_metrics["precision"],
    #                                                   'recall':test_metrics["recall"],
    #                                                   'positives':positives},
    #                                  ignore_index=True)

            df_predict2=pd.DataFrame({'memberKey':df_final['clientDSMemberKey']})
            df_predict2['HCC']=y
            df_predict2['probability']=predictor.predict_proba(df_final[features_current], model=modelchoice)[1]
            df_predict=df_predict.append(df_predict2)
    
    print("Modeling Complete")
    df_final = pd.DataFrame()
    df_predict['runDate']=dt.datetime.today().strftime('%Y-%m-%d')
    idx=df_predict.groupby(['memberKey','HCC','runDate'])['probability'].transform(max)==df_predict['probability']
    df_predict=df_predict[idx].copy()
    df_predict=df_predict.drop_duplicates()
    
    
##check for same day run and delete old data
    result = engine.execute("SELECT MAX(rundate) as rundate FROM DS_Suspecting_007_ModelOutput")
    rundate = pd.DataFrame(result.fetchall(),columns=result.keys())
    if rundate['rundate'].iloc[0] == None:
        pass
    else:
        now = dt.datetime.now().strftime('%Y-%m-%d')
        if rundate['rundate'].iloc[0].strftime('%Y-%m-%d') == dt.datetime.now().strftime('%Y-%m-%d'):
            engine.execute("DELETE FROM DS_Suspecting_007_ModelOutput WHERE runDate='{}'".format(now))
            print('Deleted old results from today!')
        else:
            print('No run yet today!')
            

##push results back into SQL
    conn = engine.connect().execution_options(autocommit=True)
    tbl='DS_Suspecting_007_ModelOutput'

    print('Pushing Suspecting Results to SQL...')
    n=1+math.floor(len(df_predict)/10000000)
    for i in range(n):
        low=10000000*i
        up=10000000*(i+1)
        df_predict1=df_predict[low:up]
        df_predict1.to_sql(tbl, engine, index=False, if_exists="append", schema="dbo")
        pDone=100*(i+1)/n
        print('{}% of Data Pushed to SQL'.format(pDone))
        print("Suspecting Complete!")
        print("**************************************************")

##run follow-up sql procs       
    if c000_proc:
        print("**************************************************")
        print("Beginning C_000_B4_ExecuteBig4")
        start = time.time()
        conn.execute("EXEC C_000_B4_ExecuteBig4_TEST 'REPLACE'")
    print("C_000_B4_ExecuteBig4 finished in {} minutes".format(np.rint((time.time()-starttime)/60)))  
        
    if e000_proc:
        print("**************************************************")
        print("Beginning E_000_Create_DataMart_Tables")
        start = time.time()
        conn.execute("EXEC E_000_Create_DataMart_Tables 'REPLACE'")
    print("E_000_Create_DataMart_Tables finished in {} minutes".format(np.rint((time.time()-starttime)/60)))  
          
    if g000_proc:
        print("**************************************************")
        print("Beginning G_000_Create_Output_Tables")
        start = time.time()
        conn.execute("EXEC G_000_Create_Output_Tables 'REPLACE'")
    print("G_000_Create_Output_Tables finished in {} minutes".format(np.rint((time.time()-starttime)/60)))  

    conn.close()

    print("**************************************************")
    print("**************************************************")
    print('done! good job! proud of you!')
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
