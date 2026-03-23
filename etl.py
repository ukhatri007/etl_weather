import pandas as pd
from  py_countries_states_cities_database import  get_all_countries
from sqlalchemy import create_engine
from countrystatecity_countries import get_cities_of_country




def countries_detail():
    country = get_all_countries()
    country_detail =[{"country_name":country["name"],"iso2":country["iso2"]} for country in country]
    return country_detail

def cityname(country_detail):
    city_full_list = []
    for cn in country_detail:
        city_all_name = get_cities_of_country(cn['iso2'])
        for city in city_all_name:
            city_full_list.append({
                "id":city.id,
                "country": cn["country_name"],
                "name":city.name,
                "latitude":city.latitude,
                "longitude":city.longitude
            })

    df=pd.DataFrame(city_full_list)
    return df
            
def api_call():
    apiKey = "&appid=80e43223a826e62159c409e5395e2c99"
    url = "https://api.openweathermap.org/data/2.5/weather?units=metric&q="
    cityname = "kathmandu"
    final_url = url+cityname+apiKey

def load_cities_df(df):
    engine = create_engine("postgresql://ujjwolkhatri:password@localhost:5432/weather_db")
    response_load=df.to_sql(
        name="city_content",        # table name in DB
        con=engine,
        schema="weather_schema",
        if_exists="replace", # 'replace'/'append'/'fail'
        index=False          # don't write DataFrame index as column
    )
    print(f"data loaded succesfully{response_load}")

if __name__ == "__main__":

    country_detail =countries_detail()
    city_name =cityname(country_detail)
    city_df = pd.DataFrame(city_name)
    load_cities_df(city_df)
    
    # print(country_detail)
