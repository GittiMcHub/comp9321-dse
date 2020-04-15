import sys
import pandas as pd
import json
import numpy as np
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import statsmodels.api as sm
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from numpy import corrcoef
from sklearn.metrics import explained_variance_score, mean_squared_error


"""
    Author: z529898
    
    Input:  training.csv validation.csv
    Output:
        z529898.PART1.summary.csv   contains the evaluation metrics (MSR,correlation) for the model trained 
                                    for the first part of the assignment
                                    MSR : the mean_squared_error in the regression problem
                                    correlation : The Pearson correlation coefficient in the regression problem
                                        zid,MSR,correlation
                                        YOUR_ZID,6.1,0.7
                                        
        z529898.PART1.output.csv    stores the predicted revenues for all of the movies in the evaluation dataset 
                                    (not training dataset)
                                        movie_id,predicted_revenue
                                        1,7655555
                                        ...
                                        
        z529898.PART2.summary.csv   contains the evaluation metrics (average_precision,average_recall,accuracy) 
                                    for the model trained for the second part of the assignment
                                    average_precision : the avg precision for all classes in the classification problem
                                    average_recall : the average recall for all classes in the classification problem
                                        zid,average_precision,average_recall,accuracy 
                                        YOUR_ZID,6.1,0.7,89
                                        
        z529898.PART2.output.csv    stores the predicted ratings for all of the movies in the evaluation dataset 
                                    (not training dataset)
                                        movie_id,predicted_rating
                                        1,1
                                        ...
"""

if len(sys.argv) != 3:
    print("Usage:", sys.argv[0], "<training.csv> <validation.csv>")
    sys.exit(1)

try:
    with open(sys.argv[1]) as f:
        print("Training data: " + f.name)
    with open(sys.argv[2]) as f:
        print("Validation data: " + f.name)
except IOError:
    print("Input files do not exist!")
    sys.exit(1)

training_csv = sys.argv[1]
validation_csv = sys.argv[2]

print("Input done")

engine = create_engine('sqlite:///./test', echo=False)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

DEBUG = True


def log(s):
    if DEBUG:
        print(s)


def json_list_to_df(fk_name, fk_value, col_prefix, json_str):
    json_list = json.loads(json_str)
    if json_str is None or len(json_str) == 0:
        return None
    col_map = {}
    try:
        temp_df_cols = json_list[0].keys()
        for col in temp_df_cols:
            col_map.update({col: col_prefix + col})
        col_map.update({fk_name: fk_name})

        # print("Data: " + json_str)
        # print("Columns: " + str(col_map))

        ret_df = pd.DataFrame(columns=col_map.values())
        for entry in json_list:
            data_row = {fk_name: [fk_value]}
            for key in entry:
                data_row.update({col_map[key]: [entry[key]]})
            temp_df = pd.DataFrame.from_dict(data_row, orient='columns')
            ret_df = pd.concat([ret_df, temp_df])
        return ret_df
    except IndexError:
        print("x")


def store_df(p_df, table_name):
    log("Storing Table: " + table_name)
    p_df.to_sql(table_name, con=engine, if_exists='replace')


def csv_to_sqlite(input_csv, table_prefix):
    df = pd.read_csv(input_csv)
    # print(df.head(1))
    cast_df = pd.DataFrame()
    crew_df = pd.DataFrame()
    genres_df = pd.DataFrame()
    keywords_df = pd.DataFrame()
    prod_companies_df = pd.DataFrame()
    prod_countries_df = pd.DataFrame()
    languages_df = pd.DataFrame()

    log("Normalize dataframe")
    for index, row in df.iterrows():
        print(".", end='')
        tmp_cast_df = json_list_to_df("movie_id", row['movie_id'], "cast_", row["cast"])
        tmp_crew_df = json_list_to_df("movie_id", row['movie_id'], "crew_", row["crew"])
        tmp_genres_df = json_list_to_df("movie_id", row['movie_id'], "genres_", row["genres"])
        tmp_keywords_df = json_list_to_df("movie_id", row['movie_id'], "keywords_", row["keywords"])
        tmp_prod_companies_df = json_list_to_df("movie_id", row['movie_id'], "production_companies_",
                                                       row["production_companies"])
        tmp_prod_countries_df = json_list_to_df("movie_id", row['movie_id'], "production_countries_",
                                                       row["production_countries"])
        tmp_languages_df = json_list_to_df("movie_id", row['movie_id'], "spoken_languages_", row["spoken_languages"])

        if tmp_cast_df is not None:
            cast_df = pd.concat([cast_df, tmp_cast_df])
        if tmp_crew_df is not None:
            crew_df = pd.concat([crew_df, tmp_crew_df])
        if tmp_genres_df is not None:
            genres_df = pd.concat([genres_df, tmp_genres_df])
        if tmp_keywords_df is not None:
            keywords_df = pd.concat([keywords_df, tmp_keywords_df])
        if tmp_prod_companies_df is not None:
            prod_companies_df = pd.concat([prod_companies_df, tmp_prod_companies_df])
        if tmp_prod_countries_df is not None:
            prod_countries_df = pd.concat([prod_countries_df, tmp_prod_countries_df])
        if tmp_languages_df is not None:
            languages_df = pd.concat([languages_df, tmp_languages_df])

    # test = json_list_to_df("movie_id", 19995, "cast_", df["cast"][0])
    log("Store DataFrames")
    df = df.filter(["movie_id","budget","homepage","original_language","original_title","overview","release_date","revenue","runtime","status","tagline","rating"])
    store_df(df, table_prefix + "movie")
    store_df(cast_df, table_prefix + "cast")
    store_df(crew_df, table_prefix + "crew")
    store_df(genres_df, table_prefix + "genres")
    store_df(keywords_df, table_prefix + "keywords")
    store_df(prod_companies_df, table_prefix + "prod_companies")
    store_df(prod_countries_df, table_prefix + "prod_countries")
    store_df(languages_df, table_prefix + "languages")
    log("Store complete")


def pre_process(input_csv, table_prefix):
    csv_to_sqlite(input_csv, table_prefix)
    with engine.begin() as connection:
        connection.execute("""DROP TABLE IF EXISTS {0}movies""".format(table_prefix))
        connection.execute("""
        create table {0}movies as
        with base as (
            SELECT m.movie_id
                 , max(revenue) as revenue
                 , max(budget) as budget
                 , max(case when homepage is null then 0 else 1 end) as has_homepage
                 --original_language,
                 , max(length(original_title))                          title_len
                 , max(length(overview)  )                              overview_len
                 , max(cast(strftime('%m', date(release_date)) as int)) release_month
                 , max(cast(strftime('%j', date(release_date)) as int)) release_day_of_year
                 , max(cast(strftime('%Y',date(release_date)) as int)) release_year
                 , max(revenue) as revenue
                 , max(runtime) as runtime
                 --status,
                 , max(length(coalesce(tagline,'')))                            as tagline_len
                 --rating
                 , count(c.cast_credit_id)                      as cast_cnt
                , group_concat(distinct c.cast_credit_id) as cast_list
            FROM {0}movie m
                     left join {0}cast c on c.movie_id = m.movie_id
            group by m.movie_id
        ), crew_cnt as (
            SELECT m.movie_id, count(cw.crew_credit_id) as crew_cnt, group_concat(distinct crew_credit_id) as crew_list
            FROM {0}movie m
                left join {0}crew cw on cw.movie_id = m.movie_id
            group by m.movie_id
        ), genres_cnt as (
            SELECT m.movie_id, count(cw.genres_id) as genres_cnt, group_concat(distinct genres_id) as genre_list
            FROM {0}movie m
                left join {0}genres cw on cw.movie_id = m.movie_id
            group by m.movie_id
        ), kw_cnt as (
            SELECT m.movie_id, count(cw.keywords_id) as kw_cnt, group_concat(distinct keywords_id) as keywords_list
            FROM {0}movie m
                left join {0}keywords cw on cw.movie_id = m.movie_id
            group by m.movie_id
        ), languages_cnt as (
            SELECT m.movie_id, count(cw.spoken_languages_iso_639_1) as languages_cnt, group_concat(distinct spoken_languages_iso_639_1) as languages_list
            FROM {0}movie m
                left join {0}languages cw on cw.movie_id = m.movie_id
            group by m.movie_id
        ), prod_companies_cnt as (
            SELECT m.movie_id, count(cw.production_companies_id) as prod_companies_cnt, group_concat(distinct cw.production_companies_id) as prod_companies_list
            FROM {0}movie m
                left join {0}prod_companies cw on cw.movie_id = m.movie_id
            group by m.movie_id
        ), prod_countries_cnt as (
            SELECT m.movie_id, count(cw.production_countries_iso_3166_1) as prod_countries_cnt, group_concat(distinct cw.production_countries_iso_3166_1) as prod_countries_list
            FROM {0}movie m
                left join {0}prod_countries cw on cw.movie_id = m.movie_id
            group by m.movie_id
        )
        SELECT base.movie_id
            ,base.revenue
            ,base.budget
            ,has_homepage
            ,title_len
            ,overview_len
            ,release_month,release_year,release_day_of_year
            ,base.runtime
            ,tagline_len
            ,cast_cnt
            ,crew_cnt
            ,genres_cnt
            ,kw_cnt
            ,languages_cnt
            ,prod_companies_cnt
            ,prod_countries_cnt
            ,m.homepage, m.original_language, m.original_title, m.overview, m.release_date,m.tagline
            ,prod_countries_list
            ,genre_list
            ,languages_list
            ,prod_companies_list
            ,keywords_list
            ,base.cast_list
            ,crew_cnt.crew_list
        from base
        join {0}movie m on m.movie_id = base.movie_id
        join crew_cnt on base.movie_id = crew_cnt.movie_id
        join genres_cnt on base.movie_id  = genres_cnt.movie_id
        join kw_cnt on base.movie_id  = kw_cnt.movie_id
        join languages_cnt on base.movie_id  = languages_cnt.movie_id
        join prod_companies_cnt on base.movie_id  = prod_companies_cnt.movie_id
        join prod_countries_cnt on base.movie_id  = prod_countries_cnt.movie_id
        """.format(table_prefix))


def get_dataset(table_prefix):
    return pd.read_sql_query("""select  revenue,
                                        budget,
                                        --has_homepage,
                                        --title_len,
                                        --overview_len,
                                        release_month,
                                        release_year,
                                        release_day_of_year,
                                        runtime,
                                        tagline_len,
                                        cast_cnt,
                                        crew_cnt,
                                        genres_cnt,
                                        kw_cnt,
                                        --languages_cnt,
                                        prod_companies_cnt
                                        --,prod_countries_cnt
                                        from {}movies
                                        where revenue > 10000
                                        """.format(table_prefix),con=engine)


def train_regression(input_csv, do_preprocess=True):
    tbl_prefix = "training_"
    if do_preprocess:
        pre_process(input_csv, tbl_prefix)

    dataset = get_dataset(tbl_prefix)

    log(dataset.describe())
    target = dataset.loc[:, dataset.columns == 'revenue']  # dependent, y
    features = dataset.loc[:, dataset.columns != 'revenue']  # independent, x

    # features_train, features_test, target_train, target_test = train_test_split(features, target, test_size=1/3, random_state=0)

    lin_reg = LinearRegression()
    lin_reg.fit(features, target)

    log(pd.DataFrame({'features': features.columns, 'coefficients': lin_reg.coef_[0]}))
    return lin_reg


def predict_regression(lin_reg, input_csv, do_preprocess=True):
    tbl_prefix = "predict_"
    if do_preprocess:
        pre_process(input_csv, tbl_prefix)

    dataset = get_dataset(tbl_prefix)
    target = dataset.loc[:, dataset.columns == 'revenue']  # dependent, y
    features = dataset.loc[:, dataset.columns != 'revenue']  # independent, x

    log(dataset.describe())
    log(pd.DataFrame({'features': features.columns, 'coefficients': lin_reg.coef_[0]}))

    prediction = reg.predict(features)
    target.insert(1, "predicted", prediction, True)
    log(target.head())
    return target


print("Training")
reg = train_regression(training_csv, do_preprocess=False)
print("Prediction")
predicted_data = predict_regression(reg, validation_csv, do_preprocess=False)

print(predicted_data.describe())
plt.scatter(predicted_data["predicted"].values, predicted_data["revenue"].values, color = 'blue')
plt.title('Revenue vs Predicted Revenue')
plt.xlabel('Actual Revenue')
plt.ylabel('Predicted Revenue')
plt.show()

#predict_regression(reg)

    #
    #
    # target_pred = regressor.predict(features_test)
    #
    # y_pred_test = regressor.predict(features_test)
    # y_pred_train = regressor.predict(features_train)
    #
    # rmse_test = np.sqrt(mean_squared_error(target_test, y_pred_test))
    # rmse_train = np.sqrt(mean_squared_error(target_train, y_pred_train))
    #
    # print("Root Mean Squared Error of Training Set: {}".format(rmse_train))
    # print("Root Mean Squared Error of Testing Set: {}".format(rmse_test))
    # print("Explained Variance: " + str(explained_variance_score(target_test, target_pred)))
    # print(target.describe())
    # print("Explained Variance: " + str(explained_variance_score(target_test, target_pred)))

# plt.scatter(X_train, y_train, color = 'blue')
# plt.plot(X_train, regressor.predict(X_train), color = 'red')
# plt.title('Revenue vs runtime (Training set)')
# plt.xlabel('runtime')
# plt.ylabel('budget')
# plt.show()
#
#
# plt.scatter(X_test, y_test, color = 'blue')
# plt.plot(X_train, regressor.predict(X_train), color = 'red')
# plt.title('Revenue vs runtime (Test set)')
# plt.xlabel('runtime')
# plt.ylabel('Revenue')
# plt.show()

