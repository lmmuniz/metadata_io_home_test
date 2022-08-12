import requests
import json
import datetime
from datetime import datetime, timezone, timedelta
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os

API_KEY = os.environ.get('API_KEY') 
DB_USER = os.environ.get('DB_USER') 
DB_PWD = os.environ.get('DB_PWD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')
DB_SCHEMA = os.environ.get('DB_SCHEMA')

LOCATIONS = ['London,England,GB', 
             'Sao Paulo,Sao Paulo,BR', 
             'Lisbon,,PT', 
             'Tokyo,,JP',
             'Orlando,FL,US',
             'San Diego,CA,US',
             'Cape Town,,ZA',
             'Mumbai,,IN',
             'Taiwan,,CN',
             'Santiago,,CL',
             ]


def get_db_connection(db_user: str, 
                      db_pwd: str,
                      db_host: str,
                      db_port: str,
                      db_name: str):
    
    try:
        engine = create_engine(
            f"postgresql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}",
            pool_size=30,
            max_overflow=30,
            pool_timeout=60,
        )
    except Exception as err:
        raise err
    return engine

def check_table_exists(pg_conn, schema, table):
        query = f"""SELECT t.table_name as table_name
                    FROM information_schema.tables t
                WHERE t.table_type = 'BASE TABLE'
                    AND t.table_schema = '{schema}'
                    AND t.table_name = '{table}'
                ORDER BY t.table_name;
                """
        try:
            df_table = pd.read_sql_query(query, pg_conn)
            if df_table.shape[0] > 0:
                print(f"Table {schema}.{table} already exist")
                return True
            else:
                print(f"Table {schema}.{table} does not exist")
                return False
        except Exception as err:
            print(err)
            pass

def load_to_postgres(pg_conn, df_pg, target_schema, target_table):
        tbl_exist = check_table_exists(pg_conn, target_schema, target_table)
        target_schema_table = target_schema + "." + target_table
        num_records = df_pg.shape[0]

        latitude = ",".join(
            ["'{}'".format(value) for value in df_pg["latitude"].unique().tolist()]
        )
        longitude = ",".join(
            ["'{}'".format(value) for value in df_pg["longitude"].unique().tolist()]
        )
        #print(df_pg['dt'])
        #df_pg['dt'] = pd.to_datetime(df_pg['dt'], unit='s')
        #print('list', df_pg['dt'].unique())
        dt = ",".join(
            ["'{}'".format(value) for value in df_pg['dt'].unique().tolist()]
        )
        #print(dt)

        if not tbl_exist:
            print(f"Creating table {target_schema_table} and loading {num_records} records")
            df_pg.to_sql(
                schema=target_schema,
                name=target_table,
                con=pg_conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
        else:
            with pg_conn.begin() as conn:
                print(f"Deleting from table {target_schema_table} where latitude in({latitude}) and longitude in({longitude}) and dt in({dt})")
                conn.execute(
                    f"DELETE FROM {target_schema_table} WHERE latitude in({latitude}) and longitude in({longitude}) and dt in({dt})"
                )

                print(f"Inserting into table {target_schema_table} total of {num_records} records")
                df_pg.to_sql(
                    schema=target_schema,
                    name=target_table,
                    con=conn,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",
                )
                print('done...')

def load_analytic_datasets(pg_conn):

    sql_statement_1 = f""" DROP TABLE IF EXISTS {DB_SCHEMA}.highest_temp"""
    sql_statement_2 = f"""CREATE TABLE {DB_SCHEMA}.highest_temp 
                            AS 
                            with base as (
                                select city_name, 
                                        dt_weather, 
                                        temp,
                                        row_number() over(partition by city_name, to_char(dt_weather, 'yyyy-mm') order by city_name, temp desc) as rn
                                from {DB_SCHEMA}.weather
                            )
                            select city_name,
                                    dt_weather,
                                    temp 
                            from base
                            where rn = 1
                            order by 1
                     """

    sql_statement_3 = f""" DROP TABLE IF EXISTS {DB_SCHEMA}.weather_date_stats"""
    sql_statement_4 = f"""CREATE TABLE {DB_SCHEMA}.weather_date_stats
                         AS
                         with base as (
                            select city_name, 
                                    dt_weather, 
                                    temp,
                                    row_number() over(partition by dt_weather order by temp desc) as rn
                            from {DB_SCHEMA}.weather
                         )
                         , max_temp_city as (
                            select dt_weather, 
                                city_name 
                            from base 
                            where rn = 1
                         )
                         select base.dt_weather, 
                                avg(temp) as avg_temp,
                                min(temp) as min_temp, 
                                max(temp) as max_temp,
                                max_temp_city.city_name as city_with_max_temp_of_day
                         from base
                                inner join
                            max_temp_city on base.dt_weather = max_temp_city.dt_weather 
                         group by 1, max_temp_city.city_name
                     """

    with pg_conn.begin() as conn:
        print(f"Creating 1st Dataset")
        conn.execute(sql_statement_1)
        conn.execute(sql_statement_2)
        
        print(f"Creating 2nd Dataset")
        conn.execute(sql_statement_3)
        conn.execute(sql_statement_4)
        print('done...')


def http_request_geocoder(
    API_KEY: str,
    CITY_NAME: str,
    STATE_CODE: str,
    COUNTRY_CODE: str,
    ) -> dict:
    if STATE_CODE == '' and CITY_NAME != '' and COUNTRY_CODE != '':
        q = CITY_NAME+','+COUNTRY_CODE
    elif STATE_CODE == '' and COUNTRY_CODE == '':
        q = CITY_NAME
    else:
        q = CITY_NAME+','+STATE_CODE+','+COUNTRY_CODE
    
    GEOCODER_CALL = f"http://api.openweathermap.org/geo/1.0/direct?q={q}&limit=1&appid={API_KEY}"
    
    try:
        #print(GEOCODER_CALL)
        resp = requests.get(GEOCODER_CALL)
        return resp.json()
    except Exception as e:
        raise e
        raise Exception(resp.text)            
        
def get_last_x_days_of_today(
    days: int
    ) -> pd.DataFrame:
    start = datetime.utcnow().date() - timedelta(days = days )
    end = datetime.utcnow().date()
    print(f"The Weather Dates to fetch from API are from ({start}) to ({end})")
    df = pd.date_range(start=start,end=end,freq='D')
    return df

def date_in_unix_epoch(date: str) -> str:
  dt = pd.to_datetime(date, utc=True)
  epoch = str(int(dt.timestamp()))
  print(f"Converting String Date ({dt}) into Unix Epoch ({epoch})")
  return epoch

def http_request_timemachine(
    API_KEY: str,
    LAT: str,
    LON: str,
    TIME: str,
    UNITS: str,
    ) -> dict:
    UNITS_CHECK = str.lower(UNITS) if str.lower(UNITS) in('standard','metric','imperial') else 'standard'

    print(f"Getting OpenWeatherMap data from LATIDUDE ({LAT}), LONGITUDE ({LON}), for DATE ({TIME}) in UNITS ({UNITS})")

    TIMEMACHINE_CALL = f"http://api.openweathermap.org/data/2.5/onecall/timemachine?lat={LATITUDE}&lon={LONGITUDE}&dt={TIME}&appid={API_KEY}&units={UNITS_CHECK}"
    
    try:
        #print(TIMEMACHINE_CALL)
        resp = requests.get(TIMEMACHINE_CALL)
        return resp.json()
    except Exception as e:
        raise e
        raise Exception(resp.text)   

def flatten_nested_json_df(
    df: pd.DataFrame
    ):
    df = df.reset_index()

    # search for columns to explode/flatten
    s = (df.applymap(type) == list).all()
    list_columns = s[s].index.tolist()

    s = (df.applymap(type) == dict).all()
    dict_columns = s[s].index.tolist()


    while (len(list_columns) > 0 or len(dict_columns) > 0):
        print(f"Columns found, lists: {list_columns}, dicts: {dict_columns}")

        new_columns = []

        for col in dict_columns:
            # explode dictionaries horizontally, adding new columns
            horiz_exploded = pd.json_normalize(df[col]).add_prefix(f"{col}_")
            horiz_exploded.index = df.index
            df = pd.concat([df, horiz_exploded], axis=1).drop(columns=[col])
            new_columns.extend(horiz_exploded.columns)  # inplace

        for col in list_columns:
            # explode lists vertically, adding new lines
            df = df.drop(columns=[col]).join(df[col].explode().to_frame())
            new_columns.append(col)

        # check if there are still dict or list fields to flatten
        s = (df[new_columns].applymap(type) == list).all()
        list_columns = s[s].index.tolist()

        s = (df[new_columns].applymap(type) == dict).all()
        dict_columns = s[s].index.tolist()

    return df

if __name__ == "__main__":
    df_final = pd.DataFrame()
    pg_conn = get_db_connection(DB_USER,DB_PWD,DB_HOST,DB_PORT,DB_NAME)
    is_exists = check_table_exists(pg_conn, DB_SCHEMA, 'weather')

    for l in LOCATIONS:
        location_list = l.split(',')
        resp = http_request_geocoder(API_KEY,location_list[0],location_list[1],location_list[2])
        CITY_NAME = resp[0].get('name', "")
        STATE_NAME = resp[0].get('state', "")
        COUNTRY_CODE = resp[0].get('country', "")
        LATITUDE = resp[0].get('lat', "")
        LONGITUDE = resp[0].get('lon', "")
        
        print('Running for:',CITY_NAME, STATE_NAME, COUNTRY_CODE, LATITUDE, LONGITUDE)
        for d in get_last_x_days_of_today(4):
            try:

                #print('Running for Date:',d)
                DT = date_in_unix_epoch(d)
                print(f"Getting Weather Info for Date ({d}) in CITY: {CITY_NAME}, STATE: {STATE_NAME}, COUNTRY: {COUNTRY_CODE}, LATITUDE: {LATITUDE}, LONGITUDE: {LONGITUDE}")
                resp2 = http_request_timemachine(API_KEY,LATITUDE,LONGITUDE, DT,'metric')
                print(resp2)
                
                if 'rain' in resp2.get('current',''):
                    del resp2['current']['rain']
                else:
                    pass
                
                if 'weather' in resp2['current']:
                    del resp2['current']['weather']
                else:
                    pass
                
                #print(resp2['current'])
                df = pd.DataFrame([resp2['current']])
                
                df_exploded = flatten_nested_json_df(df)
                df_exploded.drop(labels='index', axis='columns', inplace=True)
                df_exploded.insert(0,'city_name', CITY_NAME)
                df_exploded.insert(1,'state_name',STATE_NAME)
                df_exploded.insert(2,'country_code',COUNTRY_CODE)
                df_exploded.insert(3,'latitude',LATITUDE)
                df_exploded.insert(4,'longitude',LONGITUDE)
                
                df_final = pd.concat([df_exploded,df_final], axis=0)
            except Exception as e:
                print('An Exception Occurred in API consumption:', e)
    try: 
        df_final['dt_weather'] = df_final['dt'].apply(lambda d: datetime.utcfromtimestamp(int(d)))
        df_final['dt_sunrise'] = df_final['sunrise'].apply(lambda d: datetime.utcfromtimestamp(int(d)))
        df_final['dt_sunset'] = df_final['sunset'].apply(lambda d: datetime.utcfromtimestamp(int(d)))
    
        df_final = df_final.sort_values(['latitude','longitude','dt'])

        load_to_postgres(pg_conn,df_final,DB_SCHEMA,'weather')
        load_analytic_datasets(pg_conn)
    except Exception as e:
        print('An Exception Occurred DataFrame manipulation or Database connectivity:', e)

    
