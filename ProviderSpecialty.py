##import packages
import warnings
warnings.filterwarnings("ignore")
import sklearn
import pyodbc
import sys
import sqlalchemy
import urllib
import getpass
import re
import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import difflib
import datetime as dt
import pyodbc
import sqlalchemy
import joblib
import sys
import urllib
import time
from future.standard_library import install_aliases
import seaborn as sns
from scipy.stats import spearmanr
import matplotlib
from sqlalchemy.pool import NullPool
from numpy import unique, hstack, vstack, where, mean, std


pd.options.display.float_format = '{:20,.2f}'.format

def main():
    #pull in data from ref db
    params = urllib.parse.quote_plus('DSN=HW2WIPSQL08;DATABASE=ParetoReferenceDB;Trusted_Connection=yes')
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    result = engine.execute('SELECT * FROM Ref_provider_specialty')
    combined = pd.DataFrame(result.fetchall())
    combined.columns = result.keys()
    
    install_aliases()
    warnings.filterwarnings("ignore")
    
    # save off a snapshot of the database on the h drive just in case
    
    date_today=dt.datetime.today().strftime('%Y_%m_%d')
    csv_path= '/mnt/h/02_NoPHI/DataScience/Alicia/Provider_Specialty/ProviderSpecialty_RunningListPre_{0}.csv'.format(date_today)
    combined.to_csv(csv_path)

    ##connect to client db
    server = input("SQL Server: ")
    database_main = input("Main Database: ")
    print("List of tables and fields to check for provider specialty data in:")
    print("A_MA_004_Import_ProviderInfo (providerPrimarySpecialtyDesc, providerSecondarySpecialtyDesc fields)")
    print("A_004_Import_ProviderInfo (providerSpecialtyDesc)")
    print("A_MA_002_Import_MedicalClaims (billingProviderSpecialtyDescription, renderingProviderSpecialtyDescription)")
    print("A_02_ProviderInformation (ProviderSpecialtyDesc)")
    print("A_01_MedicalClaims_Merge_ProviderInfo_All (BillingProviderSpecialty, RenderingProviderSpecialty)")
    table = input("Table: ")
    field1 = input("Provider Specialty Field 1: ")
    field2 = input("Provider Specialty Field 2 (hit enter if no second field needed): ")
    starttime = time.time()
    cxn_str = "DSN={0};DATABASE={1};Trusted_Connection=yes".format(server, database_main)
    params = urllib.parse.quote_plus(cxn_str)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    result = engine.execute('SELECT DISTINCT {0} FROM {1}'.format(field1, table))
    
    
    df=pd.DataFrame(result.fetchall())
    specialty = pd.DataFrame()
    specialty=specialty.append(df)
    print(len(specialty))
    
    if len(field2)>3:
#         cxn_str = "DSN={0};DATABASE={1};Trusted_Connection=yes".format(server, database_main)
#         params = urllib.parse.quote_plus(cxn_str)
#         engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
        result2 = engine.execute('SELECT DISTINCT {0} FROM {1}'.format(field2, table))
        df2=pd.DataFrame(result2.fetchall())
        specialty=specialty.append(df2)
        print(len(specialty))

    print("**************************************************")
    print("Pulling specialties from SQL finished in {} minutes".format(np.rint((time.time()-starttime)/60)))
    
    df_provider=specialty.copy()
    df_provider=df_provider.reset_index(drop=True).copy()
    df_provider.columns = ['ProviderSpecialty']
    df_provider['ProviderSpecialty2'] = df_provider.ProviderSpecialty.str.replace(r"([a-z])([A-Z])", r"\1 \2").str.strip()
    df_provider['ProviderSpecialty2'] = df_provider.ProviderSpecialty2.str.lower().str.replace('/',' ').str.replace('-',' ')
    df_provider['ProviderSpecialty2'] = df_provider.ProviderSpecialty2.str.replace('va provider', 'veteran provider').str.replace('neuro', 'neuro ')
    df_provider = df_provider[df_provider['ProviderSpecialty2']!=''].copy()

    #drop duplicates
    df_provider.drop_duplicates(inplace=True)
    print("Number of distinct specialties: {0}".format(len(df_provider)))
    
    # assign roots
    roots = ['anatom','chiro',
             'diagnost', 'diet', 'electro', 'endocrin', 'epilep',  
             'genet', 'hand', 'head', 'hepatol',
             'maxillo', 'multi', 'naturo', 
             'nuclear', 'nutri', 'occupation', 'osteo', 'pain', 'perfusion', 'pharma', 'plastic', 
             'respira', 'rheum', 
             'tele']
    for root in roots:
        df_provider[root] = ((df_provider['ProviderSpecialty2'].str.contains(' '+root+'|^'+root+'|'+root, na=False)*1) &  (~df_provider['ProviderSpecialty2'].str.contains('non '+root, na=False)*1)) 
          
    df_provider['ger']=df_provider['ProviderSpecialty2'].str.contains(' ger|^ger', na=False)*1
    df_provider['uro']=df_provider['ProviderSpecialty2'].str.contains(' uro|,uro|^uro', na=False)*1
    df_provider['podia']=df_provider['ProviderSpecialty2'].str.contains(' podia|,podia|^podia| pedorthic|,pedorthic|^pedorthic', na=False)*1
    df_provider['oto']=df_provider['ProviderSpecialty2'].str.contains(' oto|otolaryn|^oto', na=False)*1
    df_provider['endocrin']=df_provider['ProviderSpecialty2'].str.contains(' endocrin|endocrin|^endocrin', na=False)*1
    df_provider['orthop']=df_provider['ProviderSpecialty2'].str.contains(' orthop|orthop|^orthop', na=False)*1
    df_provider['gastro']=df_provider['ProviderSpecialty2'].str.contains(' gastro|gastro|^gastro', na=False)*1
    df_provider['dialysis']=df_provider['ProviderSpecialty2'].str.contains(' dialysis|dialys|^dialysis', na=False)*1
    df_provider['behavior']=df_provider['ProviderSpecialty2'].str.contains(' behavior|behav|^behavior', na=False)*1
    df_provider['bariatric']=df_provider['ProviderSpecialty2'].str.contains(' bariatric|bariatric|^bariatric', na=False)*1
    df_provider['acupunctur']=df_provider['ProviderSpecialty2'].str.contains(' acupunctur|acupunct|^acupunctur', na=False)*1
    df_provider['temporomandibular']=df_provider['ProviderSpecialty2'].str.contains(' temporomandibular|temporomandibular|^temporomandibular', na=False)*1
    df_provider['speech']=df_provider['ProviderSpecialty2'].str.contains(' speech|speech|^speech', na=False)*1
    df_provider['sleep']=df_provider['ProviderSpecialty2'].str.contains(' sleep|sleep|^sleep', na=False)*1
    df_provider['hyperbaric']=df_provider['ProviderSpecialty2'].str.contains(' hyperbaric|hyperbaric|^hyperbaric', na=False)*1
    df_provider['thera']=df_provider['ProviderSpecialty2'].str.contains(' thera|therap|^thera', na=False)*1
    df_provider['infectious']=df_provider['ProviderSpecialty2'].str.contains(' infectious|infectiou|infect dis|^infectious', na=False)*1
    df_provider['pulmon']=df_provider['ProviderSpecialty2'].str.contains(' pulmon|pulmon|^pulmon', na=False)*1
    df_provider['hospit']=((df_provider['ProviderSpecialty2'].str.contains(' hospit|hospit|^hospit', na=False)*1) & (~df_provider['ProviderSpecialty2'].str.contains('non hospital', na=False))*1)
    df_provider['diab']=df_provider['ProviderSpecialty2'].str.contains(' diab|diabetes|insulin|^diab', na=False)*1
    df_provider['massage']=df_provider['ProviderSpecialty2'].str.contains(' massage|massage|^massage', na=False)*1
    df_provider['urgent']=df_provider['ProviderSpecialty2'].str.contains(' urgent|urgentcare|^urgent', na=False)*1
    df_provider['palliative']=df_provider['ProviderSpecialty2'].str.contains(' palliative|palliat|^palliative', na=False)*1
    df_provider['homeopath']=df_provider['ProviderSpecialty2'].str.contains(' homeopath|homeopath|^homeopath', na=False)*1
    df_provider['anes']=df_provider['ProviderSpecialty2'].str.contains(' anes|crna|^anes', na=False)*1
    df_provider['thora']=df_provider['ProviderSpecialty2'].str.contains(' thora|thorac|^thora', na=False)*1
    df_provider['critcare']=df_provider['ProviderSpecialty2'].str.contains("crit care|critical care", na=False)*1
    df_provider['infus']=df_provider['ProviderSpecialty2'].str.contains("infus|ivtherapy", na=False)*1
    df_provider['rehab']=df_provider['ProviderSpecialty2'].str.contains("rehab", na=False)*1
    df_provider['spin']=df_provider['ProviderSpecialty2'].str.contains(' spin|spine|^spin', na=False)*1
    df_provider['vasc']=df_provider['ProviderSpecialty2'].str.contains(' vasc|vasc|^vasc')*1
    df_provider['hem']=df_provider['ProviderSpecialty2'].str.contains(' hem|hemat|^hem')*1
    df_provider['radi']=df_provider['ProviderSpecialty2'].str.contains(' radi|diagrad|radation onc|^radi')*1
    df_provider['neuro']=df_provider['ProviderSpecialty2'].str.contains(' neuro|neuro|cerebro|^neuro')*1
    df_provider['derm']=df_provider['ProviderSpecialty2'].str.contains(' derm|dermat|^derm')*1
    df_provider['surg']=((df_provider['ProviderSpecialty2'].str.contains(' surg| asc |^asc |surgery| cfa |^cfa | cfa|surgery|^surg', na=False)*1) & (~df_provider['ProviderSpecialty2'].str.contains('no surg|non surg', na=False))*1)
    df_provider['ambul']=df_provider['ProviderSpecialty2'].str.contains(' paramedic|ambulance| ambul|ambul|^ambulance|^ambul|^paramedic')*1
    df_provider['transportation']=df_provider['ProviderSpecialty2'].str.contains(' transportation|transport|non emergent trans|^transportation')*1
    df_provider['cardiology']=df_provider['ProviderSpecialty2'].str.contains(' cardi|cardi|^cardi| heart|^heart')*1
    df_provider['clinical']=df_provider['ProviderSpecialty2'].str.contains('clinical')*1
    df_provider['clinic']=((df_provider['ProviderSpecialty2'].str.contains(' clinic| clin| center| facility| snf|snf |snf$| fac |fqhc| ctr|center|ctr|^clinic|^center|^ctr|^facility', na=False)*1) & (~df_provider['ProviderSpecialty2'].str.contains('clinical|clinician|clincal', na=False))*1)
    df_provider['colon']=df_provider['ProviderSpecialty2'].str.contains(' rectal|colrec|^colrec| colrec| colo|colon|rectal| proctol|proctolog| endoscop|^rectal|^colo|^proctol|^endoscop')*1
    df_provider['dental']=df_provider['ProviderSpecialty2'].str.contains('donti| dent| orthod|^dent|^orthod| oral|^oral')*1
    df_provider['developdisable']=df_provider['ProviderSpecialty2'].str.contains(' autism| disab|^autism|^disab')*1
    df_provider['trauma']=df_provider['ProviderSpecialty2'].str.contains('trauma |trauma|^trauma')*1
    df_provider['emerg']=((df_provider['ProviderSpecialty2'].str.contains(' emerg|^emerg|emergenc| toxicology|^toxicology', na=False)*1) & (~df_provider['ProviderSpecialty2'].str.contains('no emerg|non emerg', na=False))*1)
    df_provider['fertility']=df_provider['ProviderSpecialty2'].str.contains('fertil|^family planning|family planning|familyplanning| reproduct|reproductiv|^reproduct')*1
    df_provider['hearing']=df_provider['ProviderSpecialty2'].str.contains(' hearing|hearing| audio|^hearing|^audio')*1
    df_provider['homeHealth']=df_provider['ProviderSpecialty2'].str.contains(' home|,home|home |^home')*1
    df_provider['assistedLiving']=df_provider['ProviderSpecialty2'].str.contains(' assisted|respite care|supportive living|adult day care|adult day health|nursing home|supportive care|^assisted')*1
    df_provider['hospice']=df_provider['ProviderSpecialty2'].str.contains(' hospice|^hospic|hospic| hospic|^hospice')*1
    df_provider['imaging']=df_provider['ProviderSpecialty2'].str.contains(' mri|^mri|\(mri\)|\(pet\)|pet imag|resonance| xray|x ray| ultrasound|^xray|^ultrasound')*1
    df_provider['immune']=df_provider['ProviderSpecialty2'].str.contains(' immun|immun| allerg|allerg|^immun|^allerg')*1
    df_provider['internal']=df_provider['ProviderSpecialty2'].str.contains(' internal| int med|intrnl med|^internal|^int med')*1
    df_provider['lab']=df_provider['ProviderSpecialty2'].str.contains(' blood|blood| ^blood| microbio|^microbio|microbio| molec|^molec|molec| lab|^lab')*1
    df_provider['medsupply']=df_provider['ProviderSpecialty2'].str.contains('orthotic|prosthetic| suppli| supply| equip|dme$| prosth| orthot| shoe| wig|^orthot|^suppli|^supply|^equip|^prosth|^shoe|^wig|component| foods|^foods')*1
    df_provider['newborn']=df_provider['ProviderSpecialty2'].str.contains(' birthing| neonat|neonat| perinat| fetal|fetal| maternal| lactation|lactation|midwife|midwiv|mdwife| midwife|mid wif|^birthing|^neonat|^perinat|^fetal|^maternal|^lactation|^midwife|^midwiv|^mdwife')*1
    df_provider['none']=df_provider['ProviderSpecialty2'].str.contains('^n |^na$|null|none|ineligible provider|unspecified|undefined|not specified| not specified |not applicable|not |^not |^no |^non |miscellaneous|atypical provider| other|other |unknown|error|missing', na=False)*1
    df_provider['nurse']=df_provider['ProviderSpecialty2'].str.contains(' nurs|nursing| crn| rn | snf|snf |snf$|^nurs|^crn')*1
    df_provider['obgyn']=df_provider['ProviderSpecialty2'].str.contains('mammography|mammogra|^mammogra| mammogra| gyn|obgyn| obg|^obg|^ob | ob | obstetrics|obstetric| female|^female|female| women|^gyn|^obstetrics|^women|^urogyn')*1
    df_provider['oncology']=df_provider['ProviderSpecialty2'].str.contains(' oncolog|medonc| cancer|^oncolog|^cancer')*1
    df_provider['ophthalmology']=df_provider['ProviderSpecialty2'].str.contains(' ophthalmology| ophth|ophth|opthamology|opthalmology|^ophth|^ophthalmology|retino|retina |^retin| retin')*1
    df_provider['optometry']=df_provider['ProviderSpecialty2'].str.contains(' opto|^opto')*1
    df_provider['opti']=df_provider['ProviderSpecialty2'].str.contains(' opti|^opti')*1
    df_provider['pathology']=df_provider['ProviderSpecialty2'].str.contains(' patho| cyto| path|path$| toxicology|^patho|^cyto|^toxicology|pathology$')*1
    df_provider['PCP']=df_provider['ProviderSpecialty2'].str.contains(' physician|^physician| practice|primary care|^practice|practice|familymed|crnp| pcp|^pcp| prac|^prac| preventitive|^preventitive| intervention|^intervention| preventive|^preventive| family medicine|^family medicine| primary| preventative|^preventative')*1
    df_provider['pediatrics']=((df_provider['ProviderSpecialty2'].str.contains(' pedi|pediat| pedo| child| adolescent|adolescent|^adolescent|^pedi|^pedo|^child| peds|^peds')*1)  & (~df_provider['ProviderSpecialty2'].str.contains('pedorthic', na=False))*1)
    df_provider['physicalTherapy']=df_provider['ProviderSpecialty2'].str.contains(' physical|^physical| physia|^physia| sport|^sport|sptmed| manipulative|^manipulative')*1
    df_provider['psychiatry']=df_provider['ProviderSpecialty2'].str.contains(' psychiatry|^psychiatry| psychosom|^psychosom|psychosom| psychiatr|psychiatr| psychiatric|^psychiatric| psychiatri|^psychiatri')*1
    df_provider['psychology']=df_provider['ProviderSpecialty2'].str.contains(' psychology| social| counsel|counslr|lpc|lmsw|lcsw| mental|psychotherap|^psychology|^social|^counsel|^mental|^eating disorder| psycholog|^psycholog|psycholog| marriage|^marriage')*1
    df_provider['publicHealth']=df_provider['ProviderSpecialty2'].str.contains(' public| county| local|community| community| charit| volun|^public|^county|^local|^community|^charit|^volun|educat| educat|^educat')*1
    df_provider['renal']=df_provider['ProviderSpecialty2'].str.contains(' nephro|kidney|nephr|renal| litho|^nephro|^litho')*1
    df_provider['substance']=df_provider['ProviderSpecialty2'].str.contains('substance| substance|addict| addi|addmed|^substance|^addi| depend|dependenc|^depend| alcohol|^alcohol')*1
    df_provider['residential']=df_provider['ProviderSpecialty2'].str.contains('residential| residential|^residential')*1
    df_provider['trans']=df_provider['ProviderSpecialty2'].str.contains('transplant| transplant|transplant|^transplant')*1
    df_provider['veteran']=df_provider['ProviderSpecialty2'].str.contains('veteran| veteran|^veteran|military| military|^military|defense| defense|^defense|aerospace| aerospace|^aerospace')*1
                                
    # get dummies for whether each description contains each root
    cols=df_provider.columns[2:]
                                
    for index, row in df_provider.iterrows():
        roots_col=''
        for col in cols:
            #print(index,col)
            #print(df_provider.at[index,col])
            if df_provider.at[index,col]==1:
                roots_col=roots_col+col + ','
        df_provider.at[index, 'root_name']=roots_col                          
             
    # read in root/category combos 
    roots=pd.read_csv('/mnt/h/02_NoPHI/DataScience/Alicia/Provider_Specialty/root_cat_provider_combo.csv')
    merged=df_provider.merge(roots, how='left', on='root_name')
    merged2=merged[['ProviderSpecialty', 'ProviderSpecialty2', 'root_name', 'category1', 'category2', 'Combined']]
    check=merged2[(merged2['root_name']!='') & (pd.isna(merged2['category1']))].copy()
    if len(check)>0:
        print(str(len(check)) + ' new combinations! Review them in check.csv and add them to root_cat_provider_combo.csv and then rerun the notebook until there are no new combos. Both files can be found in h/02_NoPHI/DataScience/Alicia/Provider_Specialty/')
        check.to_csv('/mnt/h/02_NoPHI/DataScience/Alicia/Provider_Specialty/check.csv')
    else:
        print('No new root combinations. Notebook will continue running!')
        combined_final=combined.append(merged2)                 
        #drop duplicates
        combined_final.drop_duplicates(inplace=True)
        combined.drop_duplicates(inplace=True)
        print("Length of provider specialties prior to updating: {0}".format(len(combined)))
        print("Length of provider specialties after updating: {0}".format(len(combined_final)))
        print("{0} new specialties added!".format(len(combined_final)-len(combined)))
        #fill nas before inserting into SQL
        values = {'category1': '', 'category2': '', 'Combined': 'None'}
        combined_final.fillna(value=values, inplace=True)
        # write new csv with today's date
        csv_path2= '/mnt/h/02_NoPHI/DataScience/Alicia/Provider_Specialty/ProviderSpecialty_RunningListPost_{0}.csv'.format(date_today)
        combined_final.to_csv(csv_path2)
        servers=['HW2WIPSQL15']
        starttime = time.time()
        for s in servers:
            params = urllib.parse.quote_plus('DSN={0};DATABASE=ParetoReferenceDB;Trusted_Connection=yes'.format(s))
            engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            #engine.execute('DROP TABLE Ref_provider_specialty')
            #insert entire df into sql table
            combined_final.to_sql('REF_provider_specialty', con = engine, if_exists = 'append', chunksize = 1000, index=False)
        print("**************************************************")
        print("Inserting updated list to ref dbs on SQL servers finished in {} minutes".format(np.rint((time.time()-starttime)/60)))
        print("**************************************************")
        print("Now submit a Data Transfer ticket to get the table copied to PW2WIPSQL01, 'HW2WIPSQL06','HW2WIPSQL09' and 'HW2WIPSQL10'")               
                                
                                
                                
                                
                                
 
    
    ##meow when finished
        from IPython.display import Audio
        sound_file = '/mnt/h/02_NoPHI/DataScience/Alicia/Cat_Meow_2-Cat_Stevens-2034822903.wav'
        Audio(sound_file, autoplay=True)

    ##add pusheen 
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

