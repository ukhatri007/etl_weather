import json
import hashlib
from unittest import result
from sqlalchemy import inspect
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine,text
from countrystatecity_countries import get_cities_of_country, get_countries
from concurrent.futures import ThreadPoolExecutor
import random
import orchestration_utils


# Database Connection
ENGINE = create_engine("postgresql://ujjwolkhatri:password@localhost:5432/weather_db")


# ETL
def countries_detail() -> list[dict]:
    """
    --Give the detail of country listed in iso standard--
        Args: none

        Return:
        [{ "country_name": str, "iso2": str }]
    """
    country = get_countries()
    country_detail = [
        {"country_name": country.name, "iso2": country.iso2} for country in country
    ]
    return country_detail

# ETL
def cityname(country_detail) -> pd.DataFrame:
    """
    --Provides cities details of all countries--

        Args: [{ "country_name": str, "iso2": str }]

        Return:
        [{"id","iso2","country","city","latitude","longitude"}]

    """
    city_full_list = []
    for cn in country_detail:
        city_all_name = get_cities_of_country(cn["iso2"])
        for city in city_all_name:
            city_full_list.append(
                {
                    "id": city.id,
                    "iso2": cn["iso2"],
                    "country": cn["country_name"],
                    "city": city.name,
                    "latitude": city.latitude,
                    "longitude": city.longitude
                }
            )

    df = pd.DataFrame(city_full_list)
    return df


# ETL
def get_dataframe_from_postgres(query) -> pd.DataFrame:
    """
    --fetch latitude and longituded from postgres--
        Args: Query , Engine

        Return:
        pd.dataframe with column:
            -'latitude'(float)
            -'longitude'(float)


    """
    result_df = pd.read_sql(sql=query, con=ENGINE)
    return result_df


# ETL
def load_dataframe_to_postgres(dataframe: pd.DataFrame, table_name: str, if_exists: str = "append") -> int:
    """
    --Load dataframe to postgres--

        Args: dataframe, table_name,if_exists

        Return:load_response (int)

    """
    
    load_response = dataframe.to_sql(
        name=table_name,
        con=ENGINE,
        schema="weather_schema",
        if_exists=if_exists,
        index=False,
    )
    print(type(load_response))
    return load_response

# Helper
def make_hash(row):
    value = f"{row['id']}|{row['coord']}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()



def get_lat_long()->pd.DataFrame:

    """
    --this function gets query and filter the value of postgres--

        Args: none,query

        Return: dataframe
    """

    query = """
                select
                    latitude,
                    longitude 
                from weather_schema.city_list limit 1000;
            """
    df = get_dataframe_from_postgres(query=query)
    return df


def api_url(df) -> list[str]:
    """
    --This function make the list of copmlete urls to request the APIs

    Args: dataframe

    Return: urls (list)
    
    """

    """
    APIs doc: https://openweathermap.org/api/one-call-api
    """
    apiKey = "&appid=80e43223a826e62159c409e5395e2c99"
    
    
    ll_url = []

    for row in df.itertuples():
        lat = row.latitude
        lon = row.longitude

        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}"
        final_url = url + apiKey
        ll_url.append(final_url)

    suffel = random.sample(ll_url, len(ll_url))
    return suffel



def check_data_types(df):
    """
    This function takes dataframe before loading into postgres
    Checks the data type before inserting
    Checks for int, float and str. Else convert to string and load
    """
    
    # print(type(df.columns))
    for col in df.columns:
        # print(f"Column: {col}, Data Type: {df[col].dtype}")
        if df[col].dtype == "object":
                df[col] = df[col].apply(json.dumps)
    return df

def traform_data(df):
    df= df.drop_duplicates()
    df= df.copy()
    df["unique_key"] = df.apply(make_hash, axis=1)
    df["_record_loaded_at"] = datetime.now()

    return df


def chunk_url(ll_url, size):
    """
    --Generate the Urls in the chunk of list--
        Args:list_of_url,chunk_size

        Generate:chunk of 500 urls
    """
    for i in range(0, len(ll_url), size):
        yield ll_url[i : i + size]


def fetch(url):
    """
    --Request APIs for data--
        Args: url

        Return:json data
    """
    response = requests.get(url).json()
    return response

# Database Operation
def merge_data(conn):
    """--merge data from temp_table to weatehr_table--
        Args : connection/engine
        
        Returns: none"""

    with conn.begin() as conn:
       response= conn.execute(text("""
            merge into weather_schema.weather_data as t
            using weather_schema.temp_table as s 
            on t.unique_key = s.unique_key
                          
            when matched and 
	            (t.coord, t.weather, t.main, t.dt, t.base, t.name, t.visibility, t.wind, t.clouds, t.sys, t.timezone, t.rain, t.cod ) 
                is distinct from
	            (s.coord, s.weather, s.main, s.dt, s.base, s.name, s.visibility, s.wind, s.clouds, s.sys, s.timezone, s.rain, s.cod )
	                then
		            update set
                        id = s.id,
                        coord = s.coord,
                        weather = s.weather,
                        main = s.main,
                        dt = s.dt,
                        base = s.base,
                        name = s.name,
                        visibility = s.visibility,
                        wind = s.wind,
                        clouds = s.clouds,
                        sys = s.sys,
                        timezone = s.timezone,
                        rain = s.rain,
                        cod = s.cod,
                        _record_loaded_at = now()
                        
        
            when not matched then
	            insert(unique_key,coord, weather, main, dt, base, name, visibility, wind, clouds, sys, timezone, rain, cod,id, _record_loaded_at)
	            values(s.unique_key, s.coord, s.weather, s.main, s.dt, s.base, s.name, s.visibility, s.wind, s.clouds, s.sys, s.timezone, s.rain, s.cod, s.id, now())
    """))
   
    return response



def check_table_exists(engine):
    """--checks if schema and table exist in postgraces--
        Args: engine/connection

        Return: boolean value
    """
    with engine.begin() as conn: 
        result=conn.execute(text(""" 
                select 
                    case
                        when count(1) = 1
                        then true
                        else false
                    end as is_table
                from information_schema."tables" t
                where t.table_catalog = 'weather_db'
                and t.table_schema = 'weather_schema'
                and t.table_name = 'weather_data';
"""))
        return result
    
def check_table_exists(engine):
    """checks if table exits then delete the table--
        Args: engine/connection 
        Return: None"""
    with engine.begin() as conn:
        result=conn.execute(text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'weather_schema'
              AND table_name = 'temp_table'
        );
    """))
    return result
    

def delete_table(engine):
    result=check_table_exists(engine)
        
    if result.scalar():
        with engine.begin() as conn:
            conn.execute(text("""
            DROP TABLE  weather_schema.temp_table
            """))


if __name__ == "__main__":
   
#    with ENGINE.begin() as conn:
#     conn.execute(text("""
#             DELETE FROM weather_schema.temp_table
#         """))
    # country_detail = countries_detail()
    # city_name = cityname(country_detail)
    # load_dataframe_to_postgres(city_name, "city_list", "replace")
    df_lat_lon = get_lat_long()
    url = api_url(df=df_lat_lon)
    with ThreadPoolExecutor(max_workers=9) as executor:
        for chunk in chunk_url(ll_url=url, size=900):
            results = executor.map(fetch, chunk)
            data = list(results)
            df = pd.DataFrame(data)
            df = check_data_types(df=df)
            df = pd.DataFrame(df)
            df=traform_data(df=df)
            value=check_table_exists(engine=ENGINE)
            result = value.fetchall()
            bool_value=result[0][0]
            if bool_value:
                load_dataframe_to_postgres(df, "temp_table", "replace")
                merge_data(conn=ENGINE)               
            else:
                load_dataframe_to_postgres(df, "weather_data", "replace")
            
            delete_table(engine=ENGINE)
            
            