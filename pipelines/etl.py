import hashlib
import json
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import requests
from countrystatecity_countries import (
    get_cities_of_country,
    get_countries,
)
from sqlalchemy import text
from utilities.utils_postgres import (
    PostgresConnection,
    PostgresDestination,
    PostgresOperation,
    PostgresSource,
)


def cities_detail() -> pd.DataFrame:
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


# ETL
def get_dataframe_from_postgres() -> pd.DataFrame:
    """
    --fetch latitude and longituded from postgres--
        Args: Query , Engine
        Return:
        pd.dataframe with column:
            -'latitude'(float)
            -'longitude'(float)
    """
    pg_con = PostgresSource(
        database="weather_db", user="ujjwolkhatri", password="password"
    )
    query = """
                select
                    latitude,
                    longitude 
                from weather_schema.city_list limit 2000;
            """
    df = pg_con.query_postgres(query_string=query)
    return df


# Helper
def make_hash(row):
    value = f"{row['id']}|{row['coord']}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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
    Checks for list,dict and converts to string. Else load as it is.
    """
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
        )

    return df


def traform_data(dataframe):
    """This function takes dataframe before loading into postgres
    Creates unique key by combining id and coord column
    Adds _record_loaded_at column with current timestamp
    Adds rain and snow column with null value"""
    # pandas gets confued to use memory with orginal df or not which gives warning
    # so df.copy() solve this warning problem by creating now df
    df = dataframe.copy()
    df["unique_key"] = df.apply(make_hash, axis=1)
    df["_record_loaded_at"] = datetime.now()
    df["rain"] = None
    df["snow"] = None
    df = df.drop_duplicates(subset="unique_key", keep="last")

    return df


def chunk_url(ll_url, size):
    """
    --Generate the Urls in the chunk of list--
        Args:list_of_url,chunk_size
        Generate:chunk of 495 urls
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
def merge_data():
    """--merge data from temp_table to weatehr_table--
    Args : connection/engine
    Returns: none"""
    pg_conn = PostgresConnection(
        database="weather_db", user="ujjwolkhatri", password="password"
    )

    conn = pg_conn.create_conn()
    with conn.begin() as conn:
        response = conn.execute(text("""
            merge into weather_schema.weather_data as t
            using weather_schema.temp_table as s 
            on t.unique_key = s.unique_key          
                when matched and 
	                (t.coord, t.weather, t.main, t.dt, t.base, t.name, t.visibility, t.wind, t.clouds, t.sys, t.timezone, t.rain, t.cod, t.id, t.snow) 
                    is distinct from
	                (s.coord, s.weather, s.main, s.dt, s.base, s.name, s.visibility, s.wind, s.clouds, s.sys, s.timezone, s.rain, s.cod, s.id, s.snow) 
	                    then
		                    update set
                                unique_key = s.unique_key,
                                id = s.id,
                                coord = s.coord,
                                weather = s.weather,
                                main = s.main,
                                dt = s.dt,
                                base = s.base,
                                name = s.name,
                                snow = s.snow,
                                visibility = s.visibility,
                                wind = s.wind,
                                clouds = s.clouds,
                                sys = s.sys,
                                timezone = s.timezone,
                                rain = s.rain,
                                cod = s.cod,
                                _record_loaded_at = now(),
                                status = 'update'
                when not matched then
	                insert(unique_key, coord, weather, main, dt, base, name, visibility, wind, clouds, sys, timezone, rain, snow, cod,id, _record_loaded_at, status)
	                values( s.unique_key, s.coord, s.weather, s.main, s.dt, s.base, s.name, s.visibility, s.wind, s.clouds, s.sys, s.timezone, s.rain, s.snow, s.cod, s.id, now(), 'insert')
    """))

    return response


def load_country_city_to_postgres(df):
    """This function loads the city data to postgres table city_list"""
    pg_conn = PostgresDestination(
        database="weather_db", user="ujjwolkhatri", password="password"
    )
    response = pg_conn.load_to_table(df=df, table_name="city_list", if_exists="replace")
    return response


if __name__ == "__main__":

    city_name_df = cities_detail()
    load_status = load_country_city_to_postgres(df=city_name_df)

    df = get_dataframe_from_postgres()
    url = api_url(df)

    with ThreadPoolExecutor(max_workers=9) as executor:
        for chunk in chunk_url(ll_url=url, size=495):
            results = executor.map(fetch, chunk)
            data = list(results)
            df = pd.DataFrame(data)
            df = check_data_types(df=df)
            df = traform_data(dataframe=df)
            pg_connO = PostgresOperation(
                database="weather_db"
            )
            check_table = pg_connO.check_if_table_exists(
                table_name="weather_data", schema_name="weather_schema"
            )
            pg_connD = PostgresDestination(
                database="weather_db"
            )
            if check_table:
                pg_connD.load_to_table(df=df, table_name="temp_table", if_exists="replace")
                merge_data()
            else:
                pg_connD.load_to_table(df=df, table_name="weather_data", if_exists="replace")

            pg_connO.delete_table(table_name="temp_table", schema_name="weather_schema")
