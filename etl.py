import requests
import pandas as pd
from  py_countries_states_cities_database import  get_all_countries
from sqlalchemy import create_engine
from countrystatecity_countries import get_cities_of_country
import json
from multiprocessing import Pool



ENGINE = create_engine("postgresql://ujjwolkhatri:password@localhost:5432/weather_db")




def countries_detail()->list[dict]:
    country = get_all_countries()
    country_detail =[{"country_name":country["name"],"iso2":country["iso2"]} for country in country]
    return country_detail


def cityname(country_detail)->pd.DataFrame:
    city_full_list = []
    for cn in country_detail:
        city_all_name = get_cities_of_country(cn['iso2'])
        for city in city_all_name:
            city_full_list.append({
                "id":city.id,
                "country": cn["country_name"],
                "city":city.name,
                "latitude":city.latitude,
                "longitude":city.longitude
            })

    df=pd.DataFrame(city_full_list)
    return df


def load_cities_df(params):
    df = params["data"]
    table= params["table"]
    schema = params["schema"]
    df.to_sql(
        name=table,
        con=ENGINE,
        schema = schema,
        if_exists ="replace",
        index =False
    )


def get_dataframe_from_postgres(query)->pd.DataFrame:
    result_df = pd.read_sql(sql=query, con=ENGINE)
    return result_df


def load_dataframe_to_postgres(dataframe: pd.DataFrame, table_name: str, if_exists: str = "replace") -> int:
    load_response = chunk.dataframe.to_sql(
        name=table_name,
        con=ENGINE,
        schema = "weather_schema",
        if_exists = if_exists,
        index =False,
        # chunksize=50,
        # method='multi'
    )
    print(type(load_response))
    return load_response


def get_lat_long():
    query = "select cc.latitude ,cc.longitude from weather_schema.city_content cc;"
    df = get_dataframe_from_postgres(query=query)
    return df
    

def api_url(df):
    # refrence to api end point  (https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key})

    apiKey = "&appid=80e43223a826e62159c409e5395e2c99"
    ll_url=[]
    for row in df.itertuples():
        lat= row.latitude
        lon =row.longitude

        url=f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}"
        final_url = url+apiKey
        ll_url.append(final_url)
    # print(ll_url,"hi")
    return ll_url

def extract_weather_data(url):
    data=[]

    for url in url:
        df = requests.get(url).json()
        # print(df)
        data.append(df)

    return data

def convert_json_columns(df):
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
        )
    return df


if __name__ == "__main__":

    # country_detail =countries_detail()
    # city_name =cityname(country_detail)
    # city_df = pd.DataFrame(city_name)
    # load_cities_df(city_df) load_dataframe_to_postgres(city_df, <table_name>, if_exists = "append")
    df_lat_lon= get_lat_long()
    url=api_url(df=df_lat_lon)
    df = extract_weather_data(url)
    df_weather = pd.DataFrame(df)
    df_weather=convert_json_columns(df=df_weather)
    
   
    # print(df_weather)
    load_dataframe_to_postgres(df_weather,"weather_data","replace")
    # print(df)
    # load_weather= load_weatherdata(df)
    # print(url)
    
    


    
