import json
from urllib.request import urlopen
from sqlalchemy import create_engine
from flask import Flask, request
from flask_restplus import Resource, Api
import pandas as pd
import sqlite3

app = Flask(__name__)
api = Api(app=app,
          version="1.0",
          title="z5298989 - World Bank Economic Indicators ",
          description="Data service that allows a client to read and store some publicly available economic indicator data for countries around the world, and allow the consumers to access the data through a REST API. ",
          contact="d.tobaben@student.unsw.edu.au")


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
            self.engine = create_engine('sqlite:///./' + database_name, echo=False)
            self.setup_database()

    def setup_database(self):
        print("DB Setup")
        with self.engine.begin() as connection:
            connection.execute(''' CREATE TABLE IF NOT EXISTS collections (
                        id INTEGER PRIMARY KEY, 
                        creation_time DEFAULT CURRENT_TIMESTAMP,
                        min_year INTEGER,
                        max_year INTEGER,
                        indicator_id TEXT UNIQUE 
                      )
                ''')

    def store_collection(self, indicator_id, min_year, max_year, dataframe):
        data = (indicator_id, min_year, max_year)
        id = None
        with self.engine.begin() as connection:
            connection.execute(''' INSERT INTO collections (indicator_id, min_year, max_year) VALUES(?,?,?)''', data)
            result = connection.execute(''' SELECT id FROM collections where indicator_id = ?''', (indicator_id,))
            id = result.fetchone()[0]

        newdf = dataframe.drop(columns=["indicator", "country"])
        newdf["collections_id"] = id
        newdf.to_sql('collection_' + str(id), con=self.engine, if_exists='replace')
        # dtype={"indicator": str, "country": str, "countryiso3code": str, "date": int, "unit": str, "obs_status": str,
        # "indicator_id": str, "indicator_value": str, "country_id": str, "country_value": str})
        print("Stored ID: " + str(id))
        return id

    def delete_collection_by_id(self, collection_id):
        with self.engine.begin() as connection:
            connection.execute("DROP TABLE IF EXISTS collection_" + str(collection_id))
            connection.execute("DELETE FROM collections WHERE id = ?", (collection_id,))

    def get_collection_by_indicator_id(self, indicator_id):
        with self.engine.connect() as connection:
            result = connection.execute('''
                select id, creation_time, indicator_id 
                from collections 
                where indicator_id = ?''', (indicator_id,))
            return result.fetchone()

    def get_collection_by_id(self, id):
        with self.engine.connect() as connection:
            result = connection.execute('''
                select id, creation_time, indicator_id 
                from collections 
                where id = ?''', (id,))
            return result.fetchone()

    def collection_data_exists(self, id):
        if self.engine.has_table("collection_" + str(id)):
            return True
        return False

    def get_collection_data_df(self, id):
        with self.engine.connect() as connection:
            df = pd.read_sql_table(con=connection,
                                   table_name="collection_" + str(id))
            return df

    # {+id,-creation_time,-indicator}
    def get_collections_with_order(self, order_text: str):
        print("get_collections_with_order: " + order_text)
        order_text = order_text.replace("{", "").replace("}", "")
        order_values = order_text.split(",")
        order_sql = " "
        for value in order_values:
            print("Value: " + value.strip())
            if value.startswith(" "):
                order_sql = order_sql + " " + value.strip() + " ASC"
            if value.strip().startswith("-"):
                order_sql = order_sql + " " + value.replace("-", "").strip() + " DESC"
            order_sql = order_sql + ","
        order_sql = order_sql[:-1]
        with self.engine.connect() as connection:
            result = connection.execute('''
                select id, creation_time, indicator_id 
                from collections
                order by {}'''.format(order_sql))
            return result.fetchall()


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
        db = DBService.get_instance()

    def get_all_by_indicator_and_date(self, indicator, date_start, date_end):
        query_url = api_base_url + "/countries/all/indicators/" + indicator + "?per_page=10000"
        query_url = self.date_range(query_url, date_start, date_end)
        query_url = self.json(query_url)

        print(query_url)

        #json_url = urlopen(query_url)
        # TODO: durch echten request ersetzten
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
class Collections(Resource):

    @api.response(200, "OK")
    @api.doc(description="Receive a list of available collections with option to sort the results")
    @api.param("order_by", description="Orders the result. Syntax e.g: {+id,-creation_time}", type='string')
    def get(self):
        order_by: str = request.args.get("order_by")
        order_by_flag = False
        if order_by is not None and order_by != "":
            # if not order_by.startswith("{") or not order_by.endswith("}"):
            #    api.abort(400, "order_by must be in curly brackets")
            if len(order_by.split(",")) > 3:
                api.abort(400, "order_by expects only a maximum of 3 attributes")
            order_list = order_by.replace("{", "").replace("}", "").split(",")
            for value in order_list:
                if not (value.startswith(" ") or value.startswith("-")):
                    api.abort(400, "order_by value {} must have a prefix + or -".format(value))
                if value.replace("+", "").replace("-", "").strip() not in ["id", "indicator_id", "creation_time"]:
                    api.abort(400, "order_by can't handle value:{} (only: id,creation_time,indicator_id)".format(value))
            order_by_flag = True

        db: DBService = DBService.get_instance()

        result = db.get_collections_with_order(request.args.get("order_by")) if order_by_flag else db.get_collections_with_order(" id")
        output = []
        for row in result:
            output.append(
                {
                    "uri": "/collections/" + str(row["id"]),
                    "id": row["id"],
                    "creation_time": str(row["creation_time"]),
                    "indicator_id": str(row["indicator_id"])
                })
        return output

    @api.response(201, "Created")
    @api.doc(description="Import an Indicator and store it as collection")
    @api.param("indicator_id", description="Indicator to import", type='string')
    def post(self):
        # TODO: check if inidcator is an existing one (http://api.worldbank.org/v2/indicator/{indicator_id})
        indicator_id = request.args.get("indicator_id")
        if indicator_id is None or indicator_id == "":
            api.abort(400, "Parameter indicator_id is mandatory")

        ext_api: APIService = APIService()
        db: DBService = DBService.get_instance()

        result = db.get_collection_by_indicator_id(indicator_id)
        print(result)
        if result is not None:
            api.abort(409, "Indicator {} already exists in collections".format(indicator_id))

        # TODO: Fehlermbehandlung mit api.abort(40x, "aabc")
        df = pd.DataFrame(ext_api.get_all_by_indicator_and_date(indicator_id, 2012, 2017))
        df = DataTransUtils.flatten_collections_df(df)
        # TODO: deal with years
        id = db.store_collection(indicator_id, 2012, 2017, df)

        result = db.get_collection_by_id(id)

        print(result)
        return {
                "uri": "/collections/" + str(result["id"]),
                "id": result["id"],
                "creation_time": str(result["creation_time"]),
                "indicator_id": str(result["indicator_id"])
                }


@api.route('/collections/<int:id>')
class Collections(Resource):

    @api.response(200, "Ok")
    @api.doc(description="This operation retrieves a collection by its ID . The response of this operation will show the imported content from world bank API for all 6 years.")
    def get(self, id):
        db: DBService = DBService.get_instance()

        if not db.collection_data_exists(id):
            api.abort(404, "Collection with id {} not found.".format(id))

        # TODO: Handle not Found
        result_md = db.get_collection_by_id(id)
        result_df = db.get_collection_data_df(id)

        indicator = result_df["indicator_id"].max()
        indicator_value = result_df["indicator_value"].max()

        result_df = result_df.rename(columns={"country_value": "country"})
        result_df = result_df.filter(["country", "date", "value"])
        print(result_df.head())
        entries = json.loads(result_df.to_json(orient='records'))

        return {
            "id": id,
            "indicator": indicator,
            "indicator_value": indicator_value,
            "creation_time": result_md["creation_time"],
            "entries": entries
        }

    @api.response(200, "Ok")
    @api.doc(description="Delete an imported collection")
    def delete(self, id):
        db: DBService = DBService.get_instance()
        result = db.get_collection_by_id(id)
        if result is None:
            api.abort(404, "Collection id {} does not exist".format(id))

        db.delete_collection_by_id(id)
        return {
                "message": "The collection {} was removed from the database!".format(id),
                "id": id
                }


if __name__ == '__main__':
    app.run(debug=True)

    #DBService.get_instance().delete_collection_by_id(1)


#    api = APIService()
#    df = pd.DataFrame(api.get_all_by_indicator_and_date("NY.GDP.MKTP.CD", 2012, 2017))
#    df = DataTransUtils.flatten_collections_df(df)
#    id = DBService.get_instance().store_collection("NY.GDP.MKTP.CD", 2012, 2017, df)

    # print(df.head())
    # print(df[df["value"].notna()].count())
    # print(df[df["value"].isna()].count())

