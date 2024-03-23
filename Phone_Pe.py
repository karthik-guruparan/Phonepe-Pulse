from git import Repo
import shutil
import pandas as pd
import mysql.connector as mysql
import streamlit as st
import os
import json
import plotly.express as px

os.environ["GIT_PYTHON_REFRESH"] = "quiet"
clone_to_path=r"C:\Users\mikan\Documents\Karthik\GUVI\D101D102D103\data\Clones\Phonepe_Pulse"
path_data_file=r'C:\Users\mikan\Documents\Karthik\GUVI\D101D102D103\data\\'
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
    
def mysql_connect(query,data=None):
    if data is None:
        data=[]
    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='phonepe'
                            )
    cursor=connection.cursor()
    cursor.execute(query,data)
    return pd.DataFrame(cursor.fetchall())

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
                            port=3306,
                            auth_plugin='mysql_native_password'
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
st.set_page_config(layout="wide")
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
         
         with st.container(border=True, height=500):
             if year or quarter or transaction_type:
                 connection=mysql.connect(
                                     host='localhost',
                                     user='root',
                                     password='12345678',
                                     port=3306,
                                     auth_plugin='mysql_native_password'    
                                     )
                 cursor=connection.cursor()
                 transaction_type="'"+str(transaction_type)+"'"
             
                 cursor.execute(f'SELECT STATE,sum(cast(round(NUMBER_OF_TRANSACTIONS) as SIGNED))NUMBER_OF_TRANSACTIONS,SUM(CAST(round(TRANSACTION_AMOUNT)AS SIGNED)) TRANSACTION_AMOUNT  FROM phonepe.agg_trn_state WHERE YEAR IN ({year}) and QUARTER IN ({quarter}) and TRANSACTION_TYPE in ({transaction_type}) GROUP  BY 1 ORDER BY 3 DESC;')
                 data1=pd.DataFrame(cursor.fetchall(),columns=['STATE','NUMBER_OF_TRANSACTIONS','TRANSACTION_AMOUNT'])
                 data1.sort_values(by=['TRANSACTION_AMOUNT'],inplace=True)
                 data1['STATE']=data1['STATE'].map({"andaman & nicobar islands":"Andaman & Nicobar",
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
             
                 data1= data1.convert_dtypes(infer_objects=True, convert_string=True, convert_integer=True, convert_boolean=True, convert_floating=True, dtype_backend='numpy_nullable')
                 data1['NUMBER_OF_TRANSACTIONS']=data1['NUMBER_OF_TRANSACTIONS'].astype({'NUMBER_OF_TRANSACTIONS': 'int64'})
                 data1['TRANSACTION_AMOUNT']=data1['TRANSACTION_AMOUNT'].astype({'TRANSACTION_AMOUNT': 'int64'})
                 data1.rename(columns={'NUMBER_OF_TRANSACTIONS':'TRANSACTION COUNT','TRANSACTION_AMOUNT':'TRANSACTION VALUE'},inplace=True)

                 fig = px.choropleth(
                 data1,
                 geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                 featureidkey='properties.ST_NM',
                 locations='STATE',
                 color='TRANSACTION VALUE',
                 hover_data=['TRANSACTION VALUE','TRANSACTION COUNT'],
                 color_continuous_scale="Purples"
                                     )
                 fig.update_geos(fitbounds="locations", visible=False)
                 st.subheader('Trasaction value across states', anchor=None, help=None, divider=True)
                 st.plotly_chart(fig,theme='streamlit',use_container_width=True) 
                 
                 ## Year and quarter wise transactions - Horizontal stacked bar 
                 query=f'SELECT YEAR,QUARTER,sum(NUMBER_OF_TRANSACTIONS)NUMBER_OF_TRANSACTIONS,sum(TRANSACTION_AMOUNT)TRANSACTION_AMOUNT FROM phonepe.agg_trn_state WHERE TRANSACTION_TYPE in ({transaction_type}) group by 1,2 order by 1,2;'
                 df=mysql_connect(query)
                 df.rename(columns={0:'YEAR',1:'QUARTER',2:'TRANSACTION COUNT',3:'TRANSACTION VALUE'},inplace=True)
                 df['TRANSACTION COUNT']=df['TRANSACTION COUNT'].astype({'TRANSACTION COUNT': 'int64'})
                 df['TRANSACTION VALUE']=df['TRANSACTION VALUE'].astype({'TRANSACTION VALUE': 'int64'})
    #             st.write(df.dtypes)
                 fig=px.bar(df,x='YEAR',y='TRANSACTION VALUE',color='QUARTER')
                 st.plotly_chart(fig,theme='streamlit',use_container_width=True)    

                 ## Transaction value by type - Vertical bar 
                 st.subheader('Transaction value by type', anchor=None, help=None, divider=True)
                 query=f'SELECT TRANSACTION_TYPE,sum(NUMBER_OF_TRANSACTIONS)NUMBER_OF_TRANSACTIONS,sum(TRANSACTION_AMOUNT)TRANSACTION_AMOUNT FROM phonepe.agg_trn_state where YEAR={year} AND QUARTER={quarter} group by 1 order by 3 ;'
                 df=mysql_connect(query)
                 df.rename(columns={0:'Transaction type',1:'TRANSACTION COUNT',2:'TRANSACTION VALUE'},inplace=True)
                 df['TRANSACTION COUNT']=df['TRANSACTION COUNT'].astype({'TRANSACTION COUNT': 'int64'})
                 df['TRANSACTION VALUE']=df['TRANSACTION VALUE'].astype({'TRANSACTION VALUE': 'int64'})
    #             st.write(df.dtypes)
                 fig=px.bar(df,y='Transaction type',x='TRANSACTION VALUE',orientation='h',hover_data=['Transaction type','TRANSACTION VALUE','TRANSACTION COUNT'],color='TRANSACTION VALUE')
                 st.plotly_chart(fig,theme='streamlit',use_container_width=True)                
                 ## st.bar_chart(data=data1,x='STATE', y='TRANSACTION_AMOUNT', color="#ffaa00", use_container_width=True)
                 
                 with st.container():
                    col1,col2=st.columns(2)
                    with col1:
                         st.subheader('Top 10 states by transaction value', anchor=None, help=None, divider=True)
                         query=f'SELECT * FROM (SELECT STATE,sum(NUMBER_OF_TRANSACTIONS)NUMBER_OF_TRANSACTIONS,sum(TRANSACTION_AMOUNT)TRANSACTION_AMOUNT FROM phonepe.agg_trn_state WHERE YEAR IN ({year}) and QUARTER IN ({quarter}) and TRANSACTION_TYPE in ({transaction_type}) group by 1 order by 3 DESC limit 10)A ORDER BY 3;'
                         df=mysql_connect(query)
                         df.rename(columns={0:'STATE',1:'TRANSACTION COUNT',2:'TRANSACTION VALUE'},inplace=True)
                         df['TRANSACTION COUNT']=df['TRANSACTION COUNT'].astype({'TRANSACTION COUNT': 'int64'})
                         df['TRANSACTION VALUE']=df['TRANSACTION VALUE'].astype({'TRANSACTION VALUE': 'int64'})
                        
            #             st.write(df.dtypes)
                         fig=px.bar(df,y='STATE',x='TRANSACTION VALUE',orientation='h',hover_data=['STATE','TRANSACTION VALUE'],color='TRANSACTION VALUE')
                         st.plotly_chart(fig,theme='streamlit',use_container_width=True) 
                    with col2:
                         st.subheader('Bottom 10 states by transaction value', anchor=None, help=None, divider=True)
                         query=f'SELECT * FROM (SELECT STATE,sum(NUMBER_OF_TRANSACTIONS)NUMBER_OF_TRANSACTIONS,sum(TRANSACTION_AMOUNT)TRANSACTION_AMOUNT FROM phonepe.agg_trn_state WHERE YEAR IN ({year}) and QUARTER IN ({quarter}) and TRANSACTION_TYPE in ({transaction_type}) group by 1 order by 3  limit 10)A ORDER BY 3 DESC ;'
                         df=mysql_connect(query)
                         df.rename(columns={0:'STATE',1:'TRANSACTION COUNT',2:'TRANSACTION VALUE'},inplace=True)
                         df['TRANSACTION COUNT']=df['TRANSACTION COUNT'].astype({'TRANSACTION COUNT': 'int64'})
                         df['TRANSACTION VALUE']=df['TRANSACTION VALUE'].astype({'TRANSACTION VALUE': 'int64'})
            #             st.write(df.dtypes)
                         fig=px.bar(df,y='STATE',x='TRANSACTION VALUE',orientation='h',hover_data=['STATE','TRANSACTION VALUE'],color='TRANSACTION VALUE')
                         st.plotly_chart(fig,theme='streamlit',use_container_width=True) 
                 with st.container():
                    col1,col2=st.columns(2)
                    with col1:
                         st.subheader('State-wise adoption analysis', anchor=None, help=None, divider=True)
                         query=f'SELECT A.STATE,SUM(REGISTERED_USERS)REGISTERED_USERS,SUM(VIEW_COUNT)VIEW_COUNT,max(TRANSACTION_AMOUNT)TRANSACTION_AMOUNT FROM phonepe.agg_user_state A join (SELECT STATE,YEAR,QUARTER,sum(NUMBER_OF_TRANSACTIONS)NUMBER_OF_TRANSACTIONS,sum(TRANSACTION_AMOUNT)TRANSACTION_AMOUNT FROM phonepe.agg_trn_state group by 1,2,3 )B ON A.STATE=B.STATE AND A.YEAR=B.YEAR AND A.QUARTER=B.QUARTER WHERE A.YEAR IN ({year}) and A.QUARTER IN ({quarter})  GROUP BY 1 ORDER BY 4 DESC ;'
                         df=mysql_connect(query)
                         df.rename(columns={0:'STATE',1:'REGISTERED USERS',2:'VIEW COUNT',3:'TRANSACTION VALUE'},inplace=True)
                         df['REGISTERED USERS']=df['REGISTERED USERS'].astype({'REGISTERED USERS': 'int64'})
                         df['VIEW COUNT']=df['VIEW COUNT'].astype({'VIEW COUNT': 'int64'})                  
                         df['TRANSACTION VALUE']=df['TRANSACTION VALUE'].astype({'TRANSACTION VALUE': 'int64'})
            #             st.write(df.dtypes)
                         fig=px.scatter(df,y='VIEW COUNT',x='REGISTERED USERS',hover_data=['STATE','TRANSACTION VALUE','REGISTERED USERS'],color='STATE',size='TRANSACTION VALUE')
                         st.plotly_chart(fig,theme='streamlit',use_container_width=True) 

                    with col2:
                         st.subheader('Brand wise user count', anchor=None, help=None, divider=True)
                         query=f'SELECT BRAND,REGISTERED_USERS,SUM(REGISTERED_USERS) OVER() AS TOTAL_USERS,(MAX(REGISTERED_USERS)/SUM(REGISTERED_USERS) OVER())*100 PERCENT FROM(SELECT BRAND,sum(REGISTERED_USERS)REGISTERED_USERS FROM phonepe.agg_user_state_brand  WHERE YEAR IN ({year}) and QUARTER IN ({quarter}) GROUP BY 1)A GROUP BY 1,2 ORDER BY 4 ';
                         df=mysql_connect(query)
                         df.rename(columns={0:'BRAND',1:'REGISTERED USERS',2:'TOTAL USERS',3:'PERCENT'},inplace=True)
                         df['REGISTERED USERS']=df['REGISTERED USERS'].astype({'REGISTERED USERS': 'int64'})
                         df['TOTAL USERS']=df['TOTAL USERS'].astype({'TOTAL USERS': 'int64'})
                         df['PERCENT']=df['PERCENT'].astype({'PERCENT': 'float'})
            #             st.write(df.dtypes)
                         fig=px.bar(df,x='PERCENT',y='BRAND',orientation='h',hover_data=['BRAND','PERCENT','REGISTERED USERS'],color='REGISTERED USERS')
                         st.plotly_chart(fig,theme='streamlit',use_container_width=True) 

                 
    except:
        pass