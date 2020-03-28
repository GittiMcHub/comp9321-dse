import json
from urllib.request import urlopen
from sqlalchemy import create_engine
from flask import Flask
from flask_restplus import Resource, Api
import pandas as pd
import sqlite3

app = Flask(__name__)
api = Api(app)

api_base_url = "http://api.worldbank.org/v2"
database_name = "z5298989.db"


class DBService:
    # https://www.tutorialspoint.com/python_design_patterns/python_design_patterns_singleton.htm
    __instance = None
    conn = None
    engine = None

    @staticmethod
    def get_instance():
        """ Static access method. """
        if DBService.__instance is None:
            DBService()
        return DBService.__instance

    def __init__(self):
        """ Virtually private constructor. """
        if DBService.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            DBService.__instance = self
            self.engine = create_engine('sqlite:///./' + database_name, echo=True)
            self.setup_database()

    def setup_database(self):
        print("DB Setup")
        with self.engine.begin() as connection:
            connection.execute(''' CREATE TABLE IF NOT EXISTS collections (
                        id INTEGER PRIMARY KEY, 
                        insert_date DEFAULT CURRENT_TIMESTAMP,
                        min_year INTEGER,
                        max_year INTEGER,
                        indicator_id TEXT,
                        indicator_data TEXT
                      )
                ''')

    def store_collection(self, indicator_id, min_year, max_year, dataframe):
        data = (indicator_id, min_year, max_year)
        id = None
        with self.engine.begin() as connection:
            connection.execute(''' INSERT INTO collections (indicator_id, min_year, max_year) VALUES(?,?,?)''', data)
            result = connection.execute(''' SELECT id FROM collections where indicator_id = ?''', (indicator_id,))
            id = result.fetchone()[0]

        newdf = df.drop(columns=["indicator", "country"])
        newdf["collections_id"] = id
        newdf.to_sql('collection_' + str(id), con=self.engine, if_exists='replace')
                  # dtype={"indicator": str, "country": str, "countryiso3code": str, "date": int, "unit": str, "obs_status": str,
                #   "indicator_id": str, "indicator_value": str, "country_id": str, "country_value": str})
        print("Stored ID: " + str(id))


        return id

    def get_collection_by_inidcator_id(self, indicator_id):
        c = self.conn.cursor()
        return c.execute(''' INSERT INTO ''')


class DataTransUtils:
    @staticmethod
    def extract_field_from_json(json_str: json, field: str):
        return json_str[field]

    @staticmethod
    def flatten_collections_df(param_df):
        param_df["indicator_id"] = param_df["indicator"].apply(
            lambda x: DataTransUtils.extract_field_from_json(x, "id"))
        param_df["indicator_value"] = param_df["indicator"].apply(
            lambda x: DataTransUtils.extract_field_from_json(x, "value"))
        param_df["country_id"] = param_df["country"].apply(
            lambda x: DataTransUtils.extract_field_from_json(x, "id"))
        param_df["country_value"] = param_df["country"].apply(
            lambda x: DataTransUtils.extract_field_from_json(x, "value"))
        return param_df


class APIService:
    # http://api.worldbank.org/v2/countries/all/indicators/NY.GDP.MKTP.CD?date=2012:2017&format=json&per_page=1000

    latest_metadata = None

    def __init__(self):
        db = DBService()

    def get_all_by_indicator_and_date(self, indicator, date_start, date_end):
        query_url = api_base_url + "/countries/all/indicators/" + indicator + "?per_page=10000"
        query_url = self.date_range(query_url, date_start, date_end)
        query_url = self.json(query_url)

        print(query_url)

        #json_url = urlopen(query_url)
        json_url = urlopen("file:///home/dome/workspace/comp9321-dse/assignment_2/NY.GDP.MKTP.CD.json")
        result = json.loads(json_url.read())
        self.latest_metadata = result.pop(0)
        # return on error (when result.size = 1)
        return result.pop(0)

    @staticmethod
    def date_range(query, date_start, date_end):
        return query + ("" if query[-1] == "?" else "&") + "date=" + str(date_start) + (":" + str(date_end) if date_end > 0 else "")

    @staticmethod
    def json(query):
        return query + ("" if query[-1] == "?" else "&") + "format=json"

    @staticmethod
    def page(query, page_size):
        return query + ("" if query[-1] == "?" else "&") + "per_page=" + str(page_size)


@api.route('/collections')
class WorldBankEconomicInidcators(Resource):
    @staticmethod
    def get():
        return {'hello': 'world'}


if __name__ == '__main__':
    # app.run(debug=True)
    api = APIService()
    df = pd.DataFrame(api.get_all_by_indicator_and_date("NY.GDP.MKTP.CD", 2012, 2017))
    df = DataTransUtils.flatten_collections_df(df)
    id = DBService.get_instance().store_collection("NY.GDP.MKTP.CD", 2012, 2017, df)

    # print(df.head())
    # print(df[df["value"].notna()].count())
    # print(df[df["value"].isna()].count())

