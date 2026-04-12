import json
import requests
import pandas as pd

from typing import Generator
from sqlalchemy import create_engine
from countrystatecity_countries import get_cities_of_country, get_countries
from concurrent.futures import ThreadPoolExecutor

ENGINE = create_engine("postgresql://ujjwolkhatri:password@localhost:5432/weather_db")


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


def load_dataframe_to_postgres(dataframe: pd.DataFrame, table_name: str, if_exists: str = "replace") -> int:
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
                from weather_schema.city_list limit 20000;
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
    return ll_url


def convert_json_columns(df) ->pd.DataFrame:
    """
    --converts data into json formatted string--
        Args: dataframe

        Return:dataframe
    """
    for col in df.columns:
        df[col] = df[col].apply(json.dumps)
        df = df.where(df.notnull(), None)
    return df


def chunk_url(ll_url, size) -> Generator[list]:
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


if __name__ == "__main__":

    country_detail = countries_detail()
    city_name = cityname(country_detail)
    load_dataframe_to_postgres(city_name, "city_list", "replace")
    df_lat_lon = get_lat_long()
    url = api_url(df=df_lat_lon)
    with ThreadPoolExecutor(max_workers=9) as executor:
        for chunk in chunk_url(ll_url=url, size=500):
            results = executor.map(fetch, chunk)
            data = list(results)
            df = pd.DataFrame(data)
            df = convert_json_columns(df=df)
            # print(df)
            load_dataframe_to_postgres(df, "weather_new_data", "append")
        # for data in data:
        #     data=pd.DataFrame
        #     df=convert_json_columns(data)
        #     print(df)
        # load_dataframe_to_postgres(df,"weather_new_data","replace")
