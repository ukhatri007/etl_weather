
# refrence revisied code

# import pandas as pd
# from sqlalchemy import create_engine
# from py_countries_states_cities_database import get_all_countries
# from countrystatecity_countries import get_cities_of_country


# def get_country_details()->list[dict]:
#     countries = get_all_countries()
#     country_details=[{"country_name": country["name"], "iso2": country["iso2"]} for country in countries]
#     return country_details


# def get_cities(country_details)->pd.DataFrame:
#     cities_details = []
#     for country in country_details:
#         list_of_cities = get_cities_of_country(country["iso2"])
#         for city in list_of_cities:
#             cities_details.append({
#                 "id": city.id,
#                 "country_name": country["country_name"],
#                 "city_name": city.name,
#                 "latitude": city.latitude,
#                 "longitude": city.longitude
#             })
#     df= pd.DataFrame(cities_details[:2])
#     print(df)
#     # return df


# def load_cities_to_postgres(df):
#     engine = create_engine("postgresql://ujjwolkhatri:password@localhost:5432/weather_db")
#     response_load=df.to_sql(
#         name="cities",        # table name in DB
#         con=engine,
#         schema="weather_schema",
#         if_exists="replace", # 'replace'/'append'/'fail'
#         index=False          # don't write DataFrame index as column
#     )
#     print(response_load)


# if __name__ == "__main__":
#     country_details = get_country_details()
#     cities_df = get_cities(country_details)
#     # load_cities_to_postgres(df=cities_df)
