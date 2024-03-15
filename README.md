# Phonepe-Pulse
The Phonepe pulse Github repository contains a large amount of data related to various metrics and statistics. The goal is to extract this data and process it to obtain
insights and information that can be visualized in a user-friendly manner. The application needs to have following .
    
1. Extract data from the Phonepe pulse Github repository through scripting and
clone it.
2. Transform the data into a suitable format and perform any necessary cleaning
and pre-processing steps.
3. Insert the transformed data into a MySQL database for efficient storage and
retrieval.
4. Create a live geo visualization dashboard using Streamlit and Plotly in Python
to display the data in an interactive and visually appealing manner.
5. Fetch the data from the MySQL database to display in the dashboard.

## Application walkthrough
Step1: Install the following libraries and import required modules into the your code as done below.

    from git import Repo
    import shutil
    import pandas as pd
    import mysql.connector as mysql
    import streamlit as st
    import os
    import json
    import plotly.express as px

Step2: Once the modules are imported , we can clone the git repository into our own environment by using the git repo module.

    clone_to_path="E:\karthik\D101D102D103\data\Clones\Phonepe_Pulse"
    path_data_file='E:\karthik\D101D102D103\data\\'
    path_agg_trn_state=clone_to_path+r"\data\aggregated\transaction\country\india\state\\"
    path_agg_user_state=clone_to_path+r"\data\aggregated\user\country\india\state\\"
    path_map_trn_district=clone_to_path+r"\data\map\transaction\hover\country\india\state\\"
    path_map_user_district=clone_to_path+r"\data\map\user\hover\country\india\state\\"
    
    def clone_git(clone_to_path):
        try:
            Repo.clone_from("https://github.com/PhonePe/pulse",clone_to_path) 
            st.success('Git Repository Cloned')
        except:
            st.info('Repository is already cloned')   
            
Step3: The repository comprises data for multiple state, years and quarters and can be accessed using nested for loops as done below.
        
        def extract_state_transactions(path_agg_trn_state):
            # Get year-qtr-statewise Transaction type , count, value
            try:
                data={'STATE':[],'YEAR':[],'QUARTER':[],'TRANSACTION_TYPE':[],'TRANSACTIONS':[],'AMOUNT':[]}
                states=os.listdir(path_agg_trn_state)
                for state in states:
                    path_state=path_agg_trn_state+state
                    years=os.listdir(path_state)
                    for year in years:
                        path_state_year=path_state+'\\'+year
                        quarters=os.listdir(path_state_year)
                        for quarter in quarters:
                            file=open(path_state_year+'\\'+quarter)
                            df=json.load(file)
                            for item in df['data']['transactionData']:
            #                    st.write(path_state_year+'\\'+quarter)
                                data['YEAR'].append(year)
                                data['QUARTER'].append(quarter[0:1])
                                data['STATE'].append(state.replace('-',' '))
                                data['TRANSACTION_TYPE'].append(item['name'])
                                data['TRANSACTIONS'].append(item['paymentInstruments'][0]['count'])
                                data['AMOUNT'].append(item['paymentInstruments'][0]['amount'])
                return data
                
            except:
                return {'STATE':[],'YEAR':[],'QUARTER':[],'TRANSACTION_TYPE':[],'TRANSACTIONS':[],'AMOUNT':[]}

Step4: Once dataframe with necessary data is created , we can create a MySQL DB objects and insert data.
        def create_and_insert_into_tables(df_agg_trn_state,df_agg_user_state,df_agg_user_state_brand,df_map_trn_district,df_map_user_district):
                      
            connection=mysql.connect(
                                    host='localhost',
                                    user='root',
                                    password='12345678',
                                    port=3306,
                                    database='phonepe'
                                    )
            cursor=connection.cursor() 
            # STATE TABLE CREATION
            try:
                cursor.execute('DROP TABLE IF EXISTS AGG_TRN_STATE;')
                connection.commit()
                query='''
                          CREATE TABLE AGG_TRN_STATE (
                                                        YEAR INT,
                                                        QUARTER INT,
                                                        STATE VARCHAR(200),
                                                        TRANSACTION_TYPE VARCHAR(200),
                                                        NUMBER_OF_TRANSACTIONS BIGINT,
                                                        TRANSACTION_AMOUNT BIGINT
                                                    );
                     '''
                cursor.execute(query)
                connection.commit()
                values=[]
                query='''
                            insert into phonepe.AGG_TRN_STATE values(%s,%s,%s,%s,%s,%s);
                      '''
                for rownum,item in df_agg_trn_state.iterrows():
                    values.append((item['YEAR'],item['QUARTER'],item['STATE'],item['TRANSACTION_TYPE'],item['TRANSACTIONS'],item['AMOUNT']))
                cursor.executemany(query,values)
                connection.commit()
                st.success('AGG_TRN_STATE table created')
            except:
                st.error('create error- AGG_TRN_STATE')
