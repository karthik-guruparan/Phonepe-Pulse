from git import Repo
import shutil
import pandas as pd
import mysql.connector as mysql
import streamlit as st
import os
import json
import plotly.express as px

# os.environ["GIT_PYTHON_REFRESH"] = "quiet"
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
    
def mysql_connect():
    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='phonepe'
                            )
    cursor=connection.cursor()
    return cursor

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

def extract_state_user_transactions(path_agg_user_state):
    data={'STATE':[],'YEAR':[],'QUARTER':[],'REGISTERED_USERS':[],'VIEW_COUNT':[]}
    data1={'STATE':[],'YEAR':[],'QUARTER':[],'BRAND':[],'REGISTERED_USERS':[],'PERCENTAGE':[]}
    try:
        states=os.listdir(path_agg_user_state)
        for state in states:
            path_state=path_agg_user_state+state
            years=os.listdir(path_state)
            for year in years:
                path_state_year=path_state+'\\'+year
                quarters=os.listdir(path_state_year)
                for quarter in quarters:
                    file=open(path_state_year+'\\'+quarter)
                    df=json.load(file)
                    data['YEAR'].append(year)
                    data['QUARTER'].append(quarter[0:1])
                    data['STATE'].append(state.replace('-',' '))
                    data['REGISTERED_USERS'].append(df['data']['aggregated']['registeredUsers'])
                    data['VIEW_COUNT'].append(df['data']['aggregated']['appOpens'])
                    if df['data']['usersByDevice']!=None:
                        for device in df['data']['usersByDevice']:
                            data1['YEAR'].append(year)
                            data1['QUARTER'].append(quarter[0:1])
                            data1['STATE'].append(state.replace('-',' '))
                            data1['BRAND'].append(device['brand'])
                            data1['REGISTERED_USERS'].append(device['count'])
                            data1['PERCENTAGE'].append(device['percentage'])
        return data,data1
    except:
        return data,data1

def extract_district_transactions(path_map_trn_district):
    try:
        data={'YEAR':[],'QUARTER':[],'STATE':[],'DISTRICT':[],'TRANSACTIONS':[],'AMOUNT':[]}
        states=os.listdir(path_map_trn_district)
        for state in states:
            path_state=path_map_trn_district+state
            years=os.listdir(path_state)
            for year in years:
                path_state_year=path_state+'\\'+year
                quarters=os.listdir(path_state_year)
                for quarter in quarters:
                    file=open(path_state_year+'\\'+quarter)
                    df=json.load(file)
                    for item in df['data']['hoverDataList']:
                        data['YEAR'].append(year)
                        data['QUARTER'].append(quarter[0:1])
                        data['STATE'].append(state.replace('-',' '))
                        data['DISTRICT'].append(item['name'])
                        data['TRANSACTIONS'].append(item['metric'][0]['count'])
                        data['AMOUNT'].append(item['metric'][0]['amount'])
        return data
    except:
        return {'YEAR':[],'QUARTER':[],'STATE':[],'DISTRICT':[],'TRANSACTIONS':[],'AMOUNT':[]}

    
def extract_district_user_transactions(path_map_user_district):
    try:
        data={'YEAR':[],'QUARTER':[],'STATE':[],'DISTRICT':[],'REGISTERED_USERS':[],'VIEW_COUNT':[]}
        states=os.listdir(path_map_user_district)
        for state in states:
            path_state=path_map_user_district+state
            years=os.listdir(path_state)
            for year in years:
                path_state_year=path_state+'\\'+year
                quarters=os.listdir(path_state_year)
                for quarter in quarters:
                    file=open(path_state_year+'\\'+quarter)
                    df=json.load(file)
                    for district in df['data']['hoverData']:
                        data['YEAR'].append(year)
                        data['QUARTER'].append(quarter[0:1])
                        data['STATE'].append(state.replace('-',' '))
                        data['DISTRICT'].append(district)
                        data['REGISTERED_USERS'].append(df['data']['hoverData'][district]['registeredUsers'])
                        data['VIEW_COUNT'].append(df['data']['hoverData'][district]['appOpens'])
        return data
    except:
        return  {'YEAR':[],'QUARTER':[],'STATE':[],'DISTRICT':[],'REGISTERED_USERS':[],'VIEW_COUNT':[]}

    

def create_and_insert_into_tables(df_agg_trn_state,df_agg_user_state,df_agg_user_state_brand,df_map_trn_district,df_map_user_district):
    ################# GET AGGREGATED STATE TRANSACTION INFO
    
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
        


    ############################
    # Get year-qtr-statewise Users registered,Number of views and year-qtr-state-brandwise users registered and number of views

    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='phonepe'
                            )
    cursor=connection.cursor()
#     cursor=mysql_connect()    
    try:
        cursor.execute('DROP TABLE IF EXISTS AGG_USER_STATE;')
        connection.commit()
        query='''
                CREATE TABLE AGG_USER_STATE (
                                                YEAR INT,
                                                QUARTER INT,
                                                STATE VARCHAR(200),
                                                REGISTERED_USERS BIGINT,
                                                VIEW_COUNT BIGINT
                                            );
              '''
        cursor.execute(query)
        connection.commit()
        values=[]
        query='''
                  insert into phonepe.AGG_USER_STATE values(%s,%s,%s,%s,%s);
              '''
        for rownum,item in df_agg_user_state.iterrows():
            values.append((item['YEAR'],item['QUARTER'],item['STATE'],item['REGISTERED_USERS'],item['VIEW_COUNT']))
        cursor.executemany(query,values)
        connection.commit()
        st.success('AGG_USER_STATE table created')
    except:
        st.error('create error- AGG_USER_STATE')
    ############################
    # Get year-qtr-state-districtwise Transaction count and value.
    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='phonepe'
                            )
    cursor=connection.cursor()
# DISTRICT TABLE CREATION
    try:
        cursor.execute('DROP TABLE IF EXISTS MAP_TRN_DISTRICT;')
        connection.commit()
        query='''
                  CREATE TABLE MAP_TRN_DISTRICT (
                                                YEAR INT,
                                                QUARTER INT,
                                                STATE VARCHAR(200),
                                                DISTRICT VARCHAR(200),
                                                NUMBER_OF_TRANSACTIONS BIGINT,
                                                TRANSACTION_AMOUNT BIGINT
                                            );
              '''
        cursor.execute(query)
        connection.commit()
        values=[]
        query='''
                    insert into phonepe.MAP_TRN_DISTRICT values(%s,%s,%s,%s,%s,%s);
              '''
        for rownum,item in df_map_trn_district.iterrows():
            values.append((item['YEAR'],item['QUARTER'],item['STATE'],item['DISTRICT'],item['TRANSACTIONS'],item['AMOUNT']))
        cursor.executemany(query,values)
        connection.commit()  
        st.success('MAP_TRN_DISTRICT table created')

    except:
        st.error('create error- MAP_TRN_DISTRICT')
   
    #########################
    # Get year-qtr-state-districtwise Registered users and viewcount

    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='phonepe'
                            )
    cursor=connection.cursor()
# DISTRICT TABLE CREATION
    try:
        cursor.execute('DROP TABLE IF EXISTS MAP_USER_DISTRICT;')
        connection.commit()
        query='''
                CREATE TABLE MAP_USER_DISTRICT (
                                                YEAR INT,
                                                QUARTER INT,
                                                STATE VARCHAR(200),
                                                DISTRICT VARCHAR(200),
                                                REGISTERED_USERS BIGINT,
                                                VIEW_COUNT BIGINT
                                            );
              '''
       
        cursor.execute(query)
        connection.commit()
        values=[]
        query='''
                insert into phonepe.MAP_USER_DISTRICT values(%s,%s,%s,%s,%s,%s);
              '''
        for rownum,item in df_map_user_district.iterrows():
            values.append((item['YEAR'],item['QUARTER'],item['STATE'],item['DISTRICT'],item['REGISTERED_USERS'],item['VIEW_COUNT']))
        cursor.executemany(query,values)
        connection.commit()
        st.success('MAP_USER_DISTRICT table created')
    except:
        st.error('create error- MAP_USER_DISTRICT')

    ########################## get AGG_USER_STATE_BRAND
    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='phonepe'
                            )
    cursor=connection.cursor()
    try:
        cursor.execute('DROP TABLE IF EXISTS AGG_USER_STATE_BRAND;')
        connection.commit()
        query='''
                CREATE TABLE AGG_USER_STATE_BRAND (
                                                YEAR INT,
                                                QUARTER INT,
                                                STATE VARCHAR(200),
                                                BRAND VARCHAR(200),
                                                REGISTERED_USERS BIGINT,
                                                PERCENTAGE DECIMAL
                                            );
              '''
        cursor.execute(query)
        connection.commit()
        values=[]
        query='''
                insert into phonepe.AGG_USER_STATE_BRAND values(%s,%s,%s,%s,%s,%s);
              '''
        for rownum,item in df_agg_user_state_brand.iterrows():
            values.append((item['YEAR'],item['QUARTER'],item['STATE'],item['BRAND'],item['REGISTERED_USERS'],item['PERCENTAGE']))
        cursor.executemany(query,values)
        connection.commit()
        st.success('AGG_USER_STATE_BRAND table created')
        return(df_agg_trn_state,df_agg_user_state,df_map_trn_district,df_map_user_district,df_agg_user_state_brand)
    except:
        st.error('create error- MAP_USER_DISTRICT')

def create_db():
    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306
                            )
    cursor=connection.cursor()

# DB CREATION
    try:
        query='''
                  CREATE DATABASE phonepe;
              '''
        cursor.execute(query)
        connection.commit()
        st.success('MySQL DB created')
    except:
        st.info('MySQL DB exists')



## Streamlit part
st.title(':violet[Phone pe- Pulse]')
st.divider()
tab1,tab2=st.tabs(['1.Clone repository','2.Load and Visualize data'])
df_agg_trn_state=pd.DataFrame(extract_state_transactions(path_agg_trn_state))
data,data1=extract_state_user_transactions(path_agg_user_state)
df_agg_user_state=pd.DataFrame(data)
df_agg_user_state_brand=pd.DataFrame(data1)
df_map_trn_district=pd.DataFrame(extract_district_transactions(path_map_trn_district))
df_map_user_district=pd.DataFrame(extract_district_user_transactions(path_map_user_district))

with tab1:
    if st.button(":violet[Clone repository]"):
        clone_git(clone_to_path)
with tab2:
    try:
        if st.button(":violet[Load data]"):
            create_db()      
            create_and_insert_into_tables(df_agg_trn_state,df_agg_user_state,df_agg_user_state_brand,df_map_trn_district,df_map_user_district)
        
        with st.container(border=True, height=100):
            col1,col2,col3=st.columns(3)
            with col1:
               year=st.selectbox('Year',df_agg_trn_state['YEAR'].unique())
            with col2:
               quarter=st.selectbox('Quarter',df_agg_trn_state['QUARTER'].unique())
            with col3:
              transaction_type=st.selectbox('Transaction type',df_agg_trn_state['TRANSACTION_TYPE'].unique())            
            
        if year or quarter or transaction_type:
            connection=mysql.connect(
                                host='localhost',
                                user='root',
                                password='12345678',
                                port=3306
                                )
            cursor=connection.cursor()
            transaction_type="'"+str(transaction_type)+"'"

            cursor.execute(f'SELECT STATE,sum(cast(round(NUMBER_OF_TRANSACTIONS) as SIGNED))NUMBER_OF_TRANSACTIONS,SUM(CAST(round(TRANSACTION_AMOUNT)AS SIGNED)) TRANSACTION_AMOUNT  FROM phonepe.agg_trn_state WHERE YEAR IN ({year}) and QUARTER IN ({quarter}) and TRANSACTION_TYPE in ({transaction_type}) GROUP  BY 1;')
            data=pd.DataFrame(cursor.fetchall(),columns=['STATE','NUMBER_OF_TRANSACTIONS','TRANSACTION_AMOUNT'])
            data['STATE']=data['STATE'].map({"andaman & nicobar islands":"Andaman & Nicobar",
                                               "andhra pradesh":"Andhra Pradesh",
                                               "arunachal pradesh":"Arunachal Pradesh",
                                               "assam":"Assam",
                                               "bihar":"Bihar",
                                               "chandigarh":"Chandigarh",
                                               "chhattisgarh":"Chhattisgarh",
                                               "dadra & nagar haveli & daman & diu":"Dadra and Nagar Haveli and Daman and Diu",
                                               "delhi":"Delhi",
                                               "goa":"Goa",
                                               "gujarat":"Gujarat",
                                               "haryana":"Haryana",
                                               "himachal pradesh":"Himachal Pradesh",
                                               "jammu & kashmir":"Jammu & Kashmir",
                                               "jharkhand":"Jharkhand",
                                               "karnataka":"Karnataka",
                                               "kerala":"Kerala",
                                               "ladakh":"Ladakh",
                                               "lakshadweep":"Lakshadweep",
                                               "madhya pradesh":"Madhya Pradesh",
                                               "maharashtra":"Maharashtra",
                                               "manipur":"Manipur",
                                               "meghalaya":"Meghalaya",
                                               "mizoram":"Mizoram",
                                               "nagaland":"Nagaland",
                                               "odisha":"Odisha",
                                               "puducherry":"Puducherry",
                                               "punjab":"Punjab",
                                               "rajasthan":"Rajasthan",
                                               "sikkim":"Sikkim",
                                               "tamil nadu":"Tamil Nadu",
                                               "telangana":"Telangana",
                                               "tripura":"Tripura",
                                               "uttar pradesh":"Uttar Pradesh",
                                               "uttarakhand":"Uttarakhand",
                                               "west bengal":"West Bengal",})
      
            fig = px.choropleth(
            data,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='STATE',
            color='TRANSACTION_AMOUNT',
            color_continuous_scale="Reds",
            range_color=(0, 12)
                                )
            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig,theme='streamlit') 
    except:
        pass