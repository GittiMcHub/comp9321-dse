import json
import re
import time
from urllib.request import urlopen

import pandas as pd
from flask import Flask, request, jsonify
from flask_restplus import Resource, Api, fields
from sqlalchemy import create_engine

#
# Author: z5298989
#
# Required packages:
#  pandas~=1.0.3"
#  Flask~=1.1.1"
#  flask-restplus~=0.13.0"
#  SQLAlchemy~=1.3.15"
#  werkzeug~=0.16.1
#
#  !!! MAKE SURE YOU DOWNGRADE werkzeug TO VERSION 0.16.1 AT LAST !!!
#  GitHub Issue: https://github.com/noirbizarre/flask-restplus/issues/777


app = Flask(__name__)
api = Api(app=app,
          version="1.0",
          title="z5298989 - World Bank Economic Indicators ",
          description="Data service that allows a client to read and store the publicly available economic"
                      " indicator data for countries around the world, provided by the Worldbank Group."
                      " The service allows the consumers to access the data through a REST API. "
                      " The service will download the JSON data for all countries"
                      " respective to the year 2012 to 2017"
                      "Usage therefore underlies the Terms and Conditions of the Worldbank Group:"
                      " https://www.worldbank.org/en/about/legal/terms-and-conditions ",
          contact="d.tobaben@student.unsw.edu.au",
          license="MIT License & Terms of Use",
          license_url="https://github.com/GittiMcHub/comp9321-dse/blob/master/assignment_2/LICENSE",
          default="Collections",
          default_mediatype="application/json")

api_base_url = "http://api.worldbank.org/v2"
api_sleep_seconds = 1  # Sleep to avoid hitting rate limit while iterating pages
database_name = "z5298989.db"
query_year_start = 2012
query_year_end = 2017

collectionCreatedModel = api.model('CollectionCreatedModel', {
    'uri': fields.Url(description='The URL with which the imported collection can be retrieved'),
    'id': fields.Integer(description='A unique integer identifier automatically generated', min=1),
    'creation_time': fields.DateTime(description='The time the collection stored in the database'),
    'indicator_id': fields.String(description='The indicator from http://api.worldbank.org/v2/indicators')
})

collectionWithDataModel = api.model('CollectionWithDataModel', {
    'id': fields.Integer(description='A unique integer identifier automatically generated ', min=1),
    'indicator': fields.String(description='The indicator from http://api.worldbank.org/v2/indicators'),
    'indicator_value': fields.String(description='The indicator name'),
    'creation_time': fields.DateTime(description='The time the collection stored in the database'),
    'entries': fields.List(fields.Nested(api.model('CollectionDataEntryModel', {
        'country': fields.String(description="Country of the data entry"),
        'date': fields.Integer(description="Year of the data entry", min=2012),
        'value': fields.Float(description="Value of the data entry"),
    })))
})

singleEconomicIndicatorValueModel = api.model('SingleEconomicIndicatorValueModel', {
    'id': fields.Integer(description='A unique integer identifier automatically generated ', min=1),
    'indicator': fields.String(description='The indicator from http://api.worldbank.org/v2/indicators'),
    'country': fields.String(description="Country of the data entry"),
    'year': fields.Integer(description="Year of the data entry", min=2012),
    'value': fields.Float(description="Value of the data entry"),
})

topBottomModel = api.model('TopBottomModel', {
    'indicator': fields.String(description='The indicator from http://api.worldbank.org/v2/indicators'),
    'indicator_value': fields.String(description='The indicator name'),
    'entries': fields.List(fields.Nested(api.model('TopBottomDataEntryModel', {
        'country': fields.String(description="Country of the data entry"),
        'value': fields.Float(description="Value of the data entry"),
    })))
})

deletionModel = api.model('DeletionModel', {
    'id': fields.Integer(description='The unique integer of the deleted collection', min=1),
    'message': fields.String(description='Deletion message'),
})

errorModel = api.model('ErrorModel', {
    'message': fields.String(description='Error message'),
})


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
        print("DBService.setup_database")
        with self.engine.begin() as connection:
            connection.execute(''' CREATE TABLE IF NOT EXISTS collections (
                        id INTEGER PRIMARY KEY, 
                        creation_time DEFAULT CURRENT_TIMESTAMP,
                        min_year INTEGER,
                        max_year INTEGER,
                        indicator_id TEXT UNIQUE,
                        indicator_name TEXT
                      )
                ''')

    def store_collection(self, indicator_metadata, min_year, max_year, dataframe):
        data = (indicator_metadata["id"], indicator_metadata["name"], min_year, max_year)
        with self.engine.begin() as connection:
            connection.execute(''' INSERT INTO collections (indicator_id, indicator_name, min_year, max_year) 
                                    VALUES(?,?,?,?)''', data)
            result = connection.execute(''' SELECT id FROM collections 
                                            WHERE indicator_id = ?''', (indicator_metadata["id"],))
            collection_id = result.fetchone()[0]

        newdf = dataframe.drop(columns=["indicator", "country"])
        newdf["collections_id"] = collection_id
        newdf.to_sql('collection_' + str(collection_id), con=self.engine, if_exists='replace')
        # dtype={"indicator": str, "country": str, "countryiso3code": str, "date": int, "unit": str, "obs_status": str,
        # "indicator_id": str, "indicator_value": str, "country_id": str, "country_value": str})
        print("DBService.store_collection: Stored ID " + str(collection_id))
        return collection_id

    def store_empty(self, indicator_metadata):
        data = (indicator_metadata["id"], indicator_metadata["name"], query_year_start, query_year_end)
        with self.engine.begin() as connection:
            connection.execute(''' INSERT INTO collections (indicator_id,indicator_name, min_year, max_year) 
                                    VALUES(?,?,?,?)''', data)
            result = connection.execute(''' SELECT id FROM collections 
                                            WHERE indicator_id = ?''', (indicator_metadata["id"],))
            collection_id = result.fetchone()[0]
            empty_ddl = '''CREATE TABLE IF NOT EXISTS collection_{}(	
                           "index" BIGINT,
                           countryiso3code TEXT,
                           date TEXT,
                           value FLOAT,
                           unit TEXT,
                           obs_status TEXT,
                           decimal BIGINT,
                           indicator_id TEXT,
                           indicator_value TEXT,
                           country_id TEXT,
                           country_value TEXT,
                           collections_id BIGINT)'''.format(collection_id)
            connection.execute(empty_ddl)

        print("DBService.store_collection: EMPTY DATASET. Stored ID " + str(collection_id))
        return collection_id

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

    def get_collection_by_id(self, collection_id):
        with self.engine.connect() as connection:
            result = connection.execute('''
                select id, creation_time, indicator_id, indicator_name 
                from collections 
                where id = ?''', (collection_id,))
            return result.fetchone()

    def collection_data_exists(self, collection_id):
        if self.engine.has_table("collection_" + str(collection_id)):
            return True
        return False

    def get_collection_data_df(self, collection_id):
        with self.engine.connect() as connection:
            df = pd.read_sql_table(con=connection,
                                   table_name="collection_" + str(collection_id))
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

    @staticmethod
    def indicator_exists(indicator):
        url = "http://api.worldbank.org/v2/indicator/{}?format=json".format(indicator)
        json_url = urlopen(url)
        result: json = json.loads(json_url.read())
        md = result.pop(0)
        if "message" in md:
            return False
        return True

    @staticmethod
    def get_indicator_metadata(indicator):
        url = "http://api.worldbank.org/v2/indicator/{}?format=json".format(indicator)
        json_url = urlopen(url)
        result: json = json.loads(json_url.read())
        return result[1][0]

    def get_all_by_indicator_and_date(self, indicator, date_start, date_end):
        query_url = api_base_url + "/countries/all/indicators/" + indicator + "?per_page=10000"
        query_url = self.date_range(query_url, date_start, date_end)
        query_url = self.json(query_url)

        print("APIService.get_all_by_indicator_and_date: query_url=" + query_url)

        json_url = urlopen(query_url, timeout=30)
        result = json.loads(json_url.read())
        self.latest_metadata = result.pop(0)

        print("APIService.get_all_by_indicator_and_date: "
              "Current Page= " + str(self.latest_metadata["page"]) + " from= " + str(self.latest_metadata["pages"]))

        while int(self.latest_metadata["page"]) < int(self.latest_metadata["pages"]):
            next_page_no = int(self.latest_metadata["page"]) + 1
            next_page_url = self.page(query_url, next_page_no)
            print("APIService.get_all_by_indicator_and_date: next_page_url=" + next_page_url)
            json_url = urlopen(self.page(query_url, next_page_no))
            temp_result = json.loads(json_url.read())
            self.latest_metadata = temp_result.pop(0)
            temp_result_data = temp_result.pop(0)
            result[0].extend(temp_result_data)
            print("APIService.get_all_by_indicator_and_date: sleep=" + next_page_url)
            time.sleep(api_sleep_seconds)

        return result.pop(0)

    @staticmethod
    def date_range(query, date_start, date_end):
        return query + ("" if query[-1] == "?" else "&") + "date=" + str(date_start) + (
            ":" + str(date_end) if date_end > 0 else "")

    @staticmethod
    def json(query):
        return query + ("" if query[-1] == "?" else "&") + "format=json"

    @staticmethod
    def per_page(query, page_size):
        return query + ("" if query[-1] == "?" else "&") + "per_page=" + str(page_size)

    @staticmethod
    def page(query, page):
        return query + ("" if query[-1] == "?" else "&") + "page=" + str(page)


@api.route('/collections')
class Collections(Resource):

    # Question - 1: Import a collection from the data service
    @api.response(201, "Indicator successfully imported", collectionCreatedModel)
    @api.response(400, "Parameter indicator_id is mandatory", errorModel)
    @api.response(404, "Indicator does not exist in source API", errorModel)
    @api.response(409, "Indicator {} already exists in collections", errorModel)
    @api.doc(description="This operation can be considered as an on-demand 'import' operation. The service will"
                         " download the JSON data for all countries respective to the year 2012 to 2017 and"
                         " identified by the indicator id given by the user and process the content into an"
                         " internal data format and store it in the database."
                         "Usage underlies the Terms and Conditions of the Worldbank Group:"
                         "https://www.worldbank.org/en/about/legal/terms-and-conditions")
    @api.param("indicator_id", description="An Indicator to import. Must be from"
                                           " http://api.worldbank.org/v2/indicators", type='string')
    def post(self):
        indicator_id = request.args.get("indicator_id")
        if indicator_id is None or indicator_id == "":
            api.abort(400, "Parameter indicator_id is mandatory")
        if not APIService.indicator_exists(indicator_id):
            api.abort(404, "Indicator {} does not exist in source API".format(indicator_id))

        ext_api: APIService = APIService()
        db: DBService = DBService.get_instance()

        result = db.get_collection_by_indicator_id(indicator_id)
        if result is not None:
            api.abort(409, "Indicator {} already exists in collections".format(indicator_id))

        api_indicator_metadata = ext_api.get_indicator_metadata(indicator_id)
        api_result = ext_api.get_all_by_indicator_and_date(indicator_id, query_year_start, query_year_end)

        if api_result is None:
            stored_id = db.store_empty(api_indicator_metadata)
        else:
            df = pd.DataFrame(api_result)
            df = DataTransUtils.flatten_collections_df(df)
            stored_id = db.store_collection(api_indicator_metadata, query_year_start, query_year_end, df)

        result = db.get_collection_by_id(stored_id)

        return {
            "uri": "/collections/" + str(result["id"]),
            "id": result["id"],
            "creation_time": str(result["creation_time"]),
            "indicator_id": str(result["indicator_id"])
        }

    # Question 3 - Retrieve the list of available collections
    @api.response(200, "Successfully retrieved the list of available collections", collectionCreatedModel)
    @api.response(400, "order_by does not match syntax: "
                       "?order_by={+id,+creation_time,+indicator,-id,-creation_time,-indicator}")
    @api.doc(description="Receive a list of available collections with option to sort the results")
    @api.param("order_by", description="order_by is a comma separated string value to sort "
                                       "the collection based on the given criteria. Each "
                                       "segment of this value indicates how the collection "
                                       "should be sorted, and it consists of two parts (+ or -,"
                                       " and the name of column e.g., id). In each segment, + "
                                       "indicates ascending order, and - indicates descending order", type='string')
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

        result = db.get_collections_with_order(
            request.args.get("order_by")) if order_by_flag else db.get_collections_with_order(" id")
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


@api.route('/collections/<int:id>')
@api.param("id", description="The unique integer identifier automatically generated for an imported collection")
class CollectionsByID(Resource):

    # Question 2 - Deleting a collection with the data service
    @api.response(200, "Collection deleted", deletionModel)
    @api.response(404, "Collection id {} does not exist", errorModel)
    @api.doc(description="This operation deletes an existing collection from the database")
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

    # Question 4 - Retrieve a collection
    @api.response(200, "Successfully retrieved collection", collectionWithDataModel)
    @api.response(404, "Collection with id {} not found.", errorModel)
    @api.doc(description="This operation retrieves a collection by its ID . "
                         "The response of this operation will show the imported "
                         "content from world bank API for all 6 years.")
    def get(self, id):
        db: DBService = DBService.get_instance()

        if not db.collection_data_exists(id):
            api.abort(404, "Collection with id {} not found.".format(id))

        result_md = db.get_collection_by_id(id)
        result_df = db.get_collection_data_df(id)

        indicator = result_md["indicator_id"]
        indicator_value = result_md["indicator_name"]

        result_df = result_df.rename(columns={"country_value": "country"})
        result_df = result_df.filter(["country", "date", "value"])

        entries = json.loads(result_df.to_json(orient='records'))

        return {
            "id": id,
            "indicator": indicator,
            "indicator_value": indicator_value,
            "creation_time": result_md["creation_time"],
            "entries": entries
        }


@api.route('/collections/<int:id>/<int:year>/<string:country>')
@api.param("id", description="The unique integer identifier automatically generated for an imported collection")
@api.param("year", description="The year of the indicators value")
@api.param("country", description="The country of the indicators value")
class CollectionsByIDAndYearAndCountry(Resource):

    # Question 5 - Retrieve economic indicator value for given country and a year
    @api.response(200, "Successfully Retrieved economic indicator value for "
                       "given country and a year", singleEconomicIndicatorValueModel)
    @api.response(404, "Collection id, year or country not found.", errorModel)
    @api.doc(description=" Retrieve economic indicator value for given country and a year.")
    def get(self, id, year, country):
        db: DBService = DBService.get_instance()

        if not db.collection_data_exists(id):
            api.abort(404, "Collection with id {} not found.".format(id))

        result_df = db.get_collection_data_df(id)
        result_df = result_df.rename(columns={
            "country_value": "country",
            "indicator_id": "indicator",
            "collections_id": "id",
            "date": "year"
        })
        result_df = result_df.filter(["id", "indicator", "country", "year", "value"])
        result_df = result_df[result_df["year"] == str(year)]
        if not result_df["id"].count() >= 1:
            api.abort(404, "Could not find data for year: {}".format(year))

        result_df = result_df[result_df["country"] == country]
        if not result_df["id"].count() >= 1:
            api.abort(404, "Could not find data for country: {}".format(country))

        return {
            "id": id,
            "indicator": result_df["indicator"].max(),
            "country": result_df["country"].max(),
            "year": year,
            "value": result_df["value"].max()
        }


@api.route('/collections/<int:id>/<int:year>')
@api.param("id", description="The unique integer identifier automatically generated for an imported collection")
@api.param("year", description="The year of the indicators value")
class CollectionsByIDAndYear(Resource):

    #  Question 6 - Retrieve top/bottom economic indicator values for a given year
    @api.response(200, "Successfully Retrieved economic indicator value for "
                       "given country and a year", topBottomModel)
    @api.response(400, "order_by invalid - Syntax: +N, N or -N, with N between 1 and 100")
    @api.response(404, "Collection with id {} not found.")
    @api.doc(description=" Retrieve economic indicator value for given year with optional Top/Bottom query")
    @api.param("q",
               description="+N (or simply N) : Returns top N countries sorted by indicator value (highest first) "
                           "-N : Returns bottom N countries sorted by indicator value "
                           " where N can be an integer value between 1 and 100",
               type='string')
    def get(self, id, year):
        query: str = request.args.get("q")
        pattern = re.compile("^[+-]{0,1}\d{1,3}$")
        query_flag = False
        query_number = 0
        bottom_flag = False
        if query is not None and query != "":
            if not pattern.match(query.strip()):
                api.abort(400, "Parameter invalid: {} - Syntax: +N, N or -N, with N between 1 and 100".format(query))
            bottom_flag = True if "-" in query else False
            query_number = int(query.strip().replace("+", "").replace("-", ""))
            if query_number <= 0 or query_number > 100:
                api.abort(400, "Query parameter invalid: {} - must be between 1 and 100".format(query))
            query_flag = True

        db: DBService = DBService.get_instance()

        if not db.collection_data_exists(id):
            api.abort(404, "Collection with id {} not found.".format(id))

        result_md = db.get_collection_by_id(id)
        result_df = db.get_collection_data_df(id)

        indicator = result_md["indicator_id"]
        indicator_value = result_md["indicator_name"]

        result_df = result_df.rename(columns={"country_value": "country"})
        result_df = result_df[result_df["date"] == str(year)]
        result_df = result_df.filter(["country", "value"])

        entries = []
        if query_flag:
            entries = json.loads(
                result_df.sort_values("value", ascending=bottom_flag).head(query_number).to_json(orient='records'))
        else:
            entries = json.loads(result_df.to_json(orient='records'))

        return {
            "indicator": indicator,
            "indicator_value": indicator_value,
            "entries": entries
        }


@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404


@app.errorhandler(400)
def bad_request(e):
    return jsonify(error=str(e)), 400


if __name__ == '__main__':
    print("Required packages: "
          "\n\tpandas~=1.0.3"
          "\n\tFlask~=1.1.1"
          "\n\tflask-restplus~=0.13.0"
          "\n\tSQLAlchemy~=1.3.15"
          "\n\twerkzeug~=0.16.1 ")
    print("!!! MAKE SURE YOU DOWNGRADE werkzeug TO VERSION 0.16.1 AT LAST !!!")
    print("GitHub Issue: https://github.com/noirbizarre/flask-restplus/issues/777")

    app.run(debug=True)
