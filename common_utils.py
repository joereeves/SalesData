import pandas as pd
import requests
import json
import mysql.connector
import config as conf
from sqlalchemy import create_engine

mydb = mysql.connector.connect(
  host=conf.config_dic["host"],
  user=conf.config_dic["db_user"],
  password=conf.config_dic["db_pwd"],
  database=conf.config_dic["database"]
)
mycursor = mydb.cursor()
engine = create_engine("mysql+mysqlconnector://{}:{}@{}/{}".format(conf.config_dic["db_user"],
                                                                   conf.config_dic["db_pwd"],conf.config_dic["host"],
                                                                   conf.config_dic["database"]))
def remove_nonnum_null(src_df, col):
    num_check_df = src_df[pd.to_numeric(src_df[col], errors='coerce').notnull()]
    return num_check_df

def remove_negative_num(src_df, col):
    num_check_df = src_df[src_df[col] > 0]
    return num_check_df

def get_data_api(url):
    response = requests.get(url)
    data = json.loads(response.text)
    pd_df = pd.json_normalize(data)
    return pd_df


def get_weather_api(url, city):

    weather_data = requests.get(url).json()

    if weather_data['cod'] != '404':
        weather_info = weather_data['main']
        temp = weather_info['temp']
        min_temp = weather_info['temp_min']
        max_temp = weather_info['temp_max']
        weather_condition = weather_data['weather'][0]['description']

        weather_info = {
            'city': [city],
            'temp': [temp],
            'min_temp': [min_temp],
            'max_temp': [max_temp],
            'weather_condition': [weather_condition]
        }

    return weather_info

def truncate_table(table_name):
    mycursor.execute(
        "TRUNCATE TABLE {}".format(table_name))


def insert_into_table(table_name, df):
    df.to_sql(table_name,con=engine, if_exists='append', index=False)