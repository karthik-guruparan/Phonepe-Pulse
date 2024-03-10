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

def clone_git():
    try:
        
        Repo.clone_from("https://github.com/PhonePe/pulse",clone_to_path)
        
        st.success('Git Repository Cloned')
    except:
        st.info('Repository is already cloned')
    return clone_to_path,path_data_file,path_agg_trn_state,path_agg_user_state,path_map_trn_district,path_map_user_district    
    
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


def create_and_insert_into_tables(clone_to_path,path_data_file,path_agg_trn_state,path_agg_user_state,path_map_trn_district,path_map_user_district):
    #################
    # Get year-qtr-statewise Transaction type , count, value
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
    df_agg_trn_state=pd.DataFrame(data)
    
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
    data={'STATE':[],'YEAR':[],'QUARTER':[],'REGISTERED_USERS':[],'VIEW_COUNT':[]}
    data1={'STATE':[],'YEAR':[],'QUARTER':[],'BRAND':[],'REGISTERED_USERS':[],'PERCENTAGE':[]}
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

    df_agg_user_state=pd.DataFrame(data)
    df_agg_user_state_brand=pd.DataFrame(data1)
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
    df_map_trn_district=pd.DataFrame(data)
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
    df_map_user_district=pd.DataFrame(data)
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


# # This code denotes how the state names are present in phone pe repository vs that in the git geojson file.
# state_fact=[]
# state_dim=[]
# for state in df_agg_trn_state['STATE'].unique():
#     state_fact.append(state.title())

# data=open(path_data_file+'indiageojson.json')
# df_india=json.load(data)
# for i in range(0,len(df_india['features'])):
#     state_dim.append(df_india['features'][i]['properties']['ST_NM'])
# print(f' fact:\n {state_fact} \n\n dim:\n {state_dim} \n\n')


def map_viz():
    query='''
            SELECT YEAR,QUARTER,STATE,TRANSACTION_TYPE,SUM(NUMBER_OF_TRANSACTIONS)NUMBER_OF_TRANSACTIONS,SUM(TRANSACTION_AMOUNT) TRANSACTION_AMOUNT
            FROM phonepe.agg_trn_state
            GROUP  BY 1,2,3,4;
          '''
    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='phonepe'
                            )
    cursor=connection.cursor()
    cursor.execute(query)
    df_agg_trn_state=pd.DataFrame(cursor.fetchall(),columns=['YEAR','QUARTER','STATE','TRANSACTION_TYPE','NUMBER_OF_TRANSACTIONS','TRANSACTION_AMOUNT'])
    connection.commit()


    df_agg_trn_state['STATE']=df_agg_trn_state['STATE'].map({"andaman & nicobar islands":"Andaman & Nicobar",
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
        df_agg_trn_state,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='STATE',
        color='TRANSACTION_AMOUNT',
        color_continuous_scale='Blues'
    )

    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig,use_container_width=True,theme=None)


## Streamlit part
st.title(':violet[Phone pe- Pulse]')
st.divider()
tab1,tab2,tab3=st.tabs(['1.Clone repository','2.Load data to DB','3.Visualize data'])
with tab1:
    if st.button(":violet[Clone repository]"):
        clone_git()
with tab2:
    if st.button(":violet[Load data]"):
        create_db()
        create_and_insert_into_tables(clone_to_path,path_data_file,path_agg_trn_state,path_agg_user_state,path_map_trn_district,path_map_user_district)
with tab3:
    if st.button(":violet[Visualize data]"):
        map_viz()
   