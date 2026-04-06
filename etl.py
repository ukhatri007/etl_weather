import requests
import pandas as pd
from sqlalchemy import create_engine
from countrystatecity_countries import get_cities_of_country,get_countries
import json
import time
from concurrent.futures import ThreadPoolExecutor




ENGINE = create_engine("postgresql://ujjwolkhatri:password@localhost:5432/weather_db")




def countries_detail()->list[dict]:
    country = get_countries()
    country_detail =[{"country_name":country.name,"iso2":country.iso2} for country in country]
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


def get_dataframe_from_postgres(query)->pd.DataFrame:
    result_df = pd.read_sql(sql=query, con=ENGINE)
    return result_df


def load_dataframe_to_postgres(dataframe: pd.DataFrame, table_name: str, if_exists: str = "replace") -> int:
    # df -> len 1500000
    # for loop lagau, ek chhoti ma 500 rows insert
    load_response = dataframe.to_sql(
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
    query = "select latitude,longitude from weather_schema.city_list ;"
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

# def extract_weather_data(url):
#     data=[]

#     for url in url:
#         df = requests.get(url).json()
#         # print(df)
#         data.append(df)
        
#     return data





def convert_json_columns(df):
    for col in df.columns:
        df[col] = df[col].apply(json.dumps)
        df = df.where(df.notnull(), None)
    return df

chunk_size = 100
def chunk_url(ll_url,size):
    for i in range(0,len(ll_url),size):
        yield ll_url[i:i+size]

def fetch(url):
    response=requests.get(url).json()
    return response

if __name__ == "__main__":

    # country_detail =countries_detail()
    # city_name =cityname(country_detail)
    # print(city_name)
    # load_dataframe_to_postgres(city_name,"city_list","replace")
    df_lat_lon= get_lat_long()
    url=api_url(df=df_lat_lon)
    with ThreadPoolExecutor(max_workers=9) as executor:
        for chunk in chunk_url(ll_url=url,size=500):
            results = executor.map(fetch, chunk)
            data=(list(results))
            df=pd.DataFrame(data)
            df=convert_json_columns(df=df)
            # print(df)
            load_dataframe_to_postgres(df,"weather_new_data","append")
        # for data in data:
        #     data=pd.DataFrame
        #     df=convert_json_columns(data)
        #     print(df)
        # load_dataframe_to_postgres(df,"weather_new_data","replace")


    
    
        
      




   

    
        
    


    

    
    
    


    
