from sqlalchemy import create_engine, text
import pandas as pd
from dataclasses import dataclass

@dataclass
class PostgresConnection:
    """
    This class is responsible for creating a connection to the PostgreSQL database.
    """
    database:str
    user:str
    password:str 

    def create_conn(self):
        self.conn = create_engine(f"postgresql://{self.user}:{self.password}@localhost:5432/{self.database}")
        return self.conn


@dataclass
class PostgresOperation:
    """
    This class is responsible for performing operations on the PostgreSQL database.
    """
    database: str
    user: str
    password: str

    def __post_init__(self):
        pg_conn = PostgresConnection(database=self.database, user=self.user, password=self.password)
        self.pg_engine = pg_conn.create_conn()


    def query_postgres(self, query_string):
        df= pd.read_sql_query(query_string, self.pg_engine)
        return df


    def check_if_table_exists(self, table_name: str,schema_name:str):
        query = text(f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables 
                WHERE table_schema = '{schema_name}' 
                AND table_name = '{table_name}'
            );
        """)
        with self.pg_engine.begin() as conn:
            result = conn.execute(query).scalar()
            return result


    def delete_table(self,table_name:str,schema_name:str):
        result = self.check_if_table_exists(table_name, schema_name)
        if result:
            query = text(f"""
                DROP TABLE {schema_name}.{table_name}
            """)
            with self.pg_engine.begin() as conn:
                conn.execute(query)
                print(f"Table {schema_name}.{table_name} has been deleted.")
        else:
            print(f"Table {schema_name}.{table_name} does not exist.")

@dataclass
class PostgresDestination():
    """
    This class is responsible for loading data to the PostgreSQL database.
    """
    database: str
    user: str
    password: str

    def __post_init__(self):
        pg_conn = PostgresConnection(database=self.database, user=self.user, password=self.password)
        self.pg_engine= pg_conn.create_conn()
        

    def load_to_table(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> int:
        load_response= df.to_sql(
            name=table_name,
            con=self.pg_engine,
            schema="weather_schema",
            if_exists=if_exists,
            index=False,    
        )
        print(type(load_response))
        return load_response


class PostgresSource():
    """
    This class is responsible for fetching data from the PostgreSQL database.
    """
    query_string:str

    database: str
    user: str
    password: str

    def __post_init__(self):
        pg_conn = PostgresConnection(database=self.database, user=self.user, password=self.password)
        self.pg_engine= pg_conn.create_conn()


    def query_postgres(self, query_string):
        df= pd.read_sql_query(query_string, self.pg_engine)
        return df
    
    
