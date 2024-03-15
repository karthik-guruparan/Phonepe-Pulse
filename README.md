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
