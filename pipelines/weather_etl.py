import hashlib
import json
import os
import pandas as pd
import requests
from datetime import datetime,timezone
from countrystatecity_countries import (
    get_cities_of_country,
    get_countries,
)
from utilities.utils_snowflake import SnowflakeOperation, SnowflakeDestination
from concurrent.futures import ThreadPoolExecutor


def cities_detail() -> pd.DataFrame:
    """--Fetch the city details from third party function  using get_countries and get_cities_of_country--
    Args: iso2 code of country
    return: dataframe with column:
    """
    country = get_countries()
    country_detail = [
        {"country_name": country.name, "iso2": country.iso2} for country in country
    ]
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
                    "longitude": city.longitude,
                }
            )
    city_full_list = pd.DataFrame(city_full_list)
    return city_full_list


def get_cities_coordinates(schema_name: str, table_name: str) -> pd.DataFrame:
    """--Gets the latitude and longitude from Snowflake table.--
    Args:
        schema_name (str): The name of the Snowflake schema.
        table_name (str): The name of the Snowflake table.
    Returns: pd.DataFrame.
    """
    conn = SnowflakeOperation(database="WEATHER_DB", schema="SCHEMA_WEATHER")
    query = f"""
                SELECT * FROM {schema_name}.{table_name};
            """
    df = conn.query_df(query_string=query)
    return df


def create_url(df) -> list:
    """--Generate url from the latitude and longitude using snowflake table--
    Args: dataframe with column latitude and longitude
    Return: list of url"""
    apiKey = os.getenv("API_KEY")
    final_url = []
    for row in df.itertuples():
        latitude = row.LATITUDE
        longitude = row.LONGITUDE
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}{apiKey}"
        final_url.append(url)
    return final_url


def chunk_url(ll_url, size):
    """
    --Generate the Urls in the chunk of list--
        Args:list_of_url,chunk_size
        Generate:chunk of 495 urls
        Note: Generator have no return type
    """
    for i in range(0, len(ll_url), size):
        yield ll_url[i : i + size]


def fetch(url) -> json:
    """
    --Request APIs for data--
        Args: url

        Return:json data
    """
    response = requests.get(url).json()
    return response


def make_hash(row):
    """--Generate a unique hash value for each row based on the 'id' and 'coord' columns.--
    Args: A row from the Snowflake DataFrame.
    Returns: str: The unique hash value.
    """
    value = f"{row['ID']}|{row['COORD']}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def transform_data(dataframe) -> pd.DataFrame:
    """function"""
    df = dataframe.copy()
    df.columns = df.columns.str.upper()
    df["SNOW"] = None
    df["RAIN"] = None
    df["UNIQUE_KEY"] = df.apply(make_hash, axis=1)
    df["_CREATED_AT"] = datetime.now(timezone.utc)
    df= df.sort_values("_CREATED_AT")
    df = df.drop_duplicates(subset="UNIQUE_KEY", keep="last")
    df = df.reset_index(drop=True)
    return df


if __name__ == "__main__":
    df = cities_detail()
    df.columns = df.columns.str.upper()

    conn = SnowflakeDestination(database="WEATHER_DB", schema="SCHEMA_WEATHER")
    conn.load_into_snowflake(df, "CITY_DETAILS", "SCHEMA_WEATHER")

    df = get_cities_coordinates("SCHEMA_WEATHER", "CITY_DETAILS")
    df_urls = create_url(df)

    with ThreadPoolExecutor(max_workers=9) as executor:
        for chunk in chunk_url(ll_url=df_urls, size=495):
            results = executor.map(fetch, chunk)
            data = list(results)
            dataframe = pd.DataFrame(data)
            df = transform_data(dataframe)
            conn_des = SnowflakeDestination(
                database="WEATHER_DB", schema="SCHEMA_WEATHER"
            )
            conn_des.load_into_snowflake(df, "WEATHER_API_DATA", "SCHEMA_WEATHER")
