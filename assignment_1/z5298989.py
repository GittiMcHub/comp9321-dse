import ast
import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os

studentid = os.path.basename(sys.modules[__name__].__file__)


#################################################
# Your personal methods can be here ...
#################################################
def success_impact_function(row):
    return (row.revenue - row.budget) / row.budget


def cast_characters_json_to_sorted_csv(json_str: str):
    print(json_str)
    # Convert to real json:  'character' => "character"
    json_str = json_str.replace("'", "\"")
    json_str = json_str.replace("INTEXT_DOUBLEQUOTE", "\\\"")
    json_str = json_str.replace("INTEXT_SINGLEQUOTE", "'")
    json_str = json_str.replace("None", "\"null\"")

    print(json_str)
    json_array = json.loads(json_str)
    char_list = []
    for char_json in json_array:
        char_list.append(char_json["character"])
    return "".join(sorted(char_list))


def log(question, output_df, other):
    print("--------------- {}----------------".format(question))
    if other is not None:
        print(question, other)
    if output_df is not None:
        print(output_df.head(5).to_string())


def question_1(movies, credits):
    """
    :param movies: the path for the movie.csv file
    :param credits: the path for the credits.csv file
    :return: df1
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################

    # Read path and set id as Index
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
    #movies_df = pd.read_csv(movies, index_col="id")
    #credits_df = pd.read_csv(credits, index_col="id")
    movies_df = pd.read_csv(movies)
    credits_df = pd.read_csv(credits)

    # Join the two datasets based on the "id" columns in the datasets,
    # keeping the rows as long as there is a match between the id columns of both dataset
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/merging.html
    df1 = movies_df.merge(credits_df, on="id", how="inner")

    log("QUESTION 1", output_df=df1, other=df1.shape)
    return df1


def question_2(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df2
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################
    # Keep the following columns in the resultant dataframe (remove the rest of columns from the result dataset):
    # 'id', title', 'popularity', 'cast', 'crew', 'budget', 'genres', 'original_language', 'production_companies', 'production_countries', 'release_date', 'revenue', 'runtime', 'spoken_languages', 'vote_average', 'vote_count'
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.filter.html
    # Using Filters as it is more readable than inplace syntax
    df2 = df1.filter(["id", "title", "popularity", "cast", "crew", "budget", "genres", "original_language", "production_companies", "production_countries", "release_date", "revenue", "runtime", "spoken_languages", "vote_average", "vote_count"])

    log("QUESTION 2", output_df=df2, other=(len(df2.columns), sorted(df2.columns)))
    return df2


def question_3(df2):
    """
    :param df2: the dataframe created in question 2
    :return: df3
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################
    # Set the index of the resultant dataframe as 'id'.
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.set_index.html
    df3 = df2.set_index("id")


    log("QUESTION 3", output_df=df3, other=df3.index.name)
    return df3


def question_4(df3):
    """
    :param df3: the dataframe created in question 3
    :return: df4
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################
    # Drop all rows where the budget is 0
    # https://chrisalbon.com/python/data_wrangling/pandas_dropping_column_and_rows/
    df4 = df3[df3.budget != 0]

    log("QUESTION 4", output_df=df4, other=(df4['budget'].min(), df4['budget'].max(), df4['budget'].mean()))
    return df4


def question_5(df4):
    """
    :param df4: the dataframe created in question 4
    :return: df5
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################
    # assume that there is a ranking scheme for movies defined by
    # (revenue - budget)/budget
    # Add a new column for the dataframe, and name it "success_impact",
    # and calculate it for each movie based on the given formula.
    # https://stackoverflow.com/a/46570641
    df5 = df4
    success_impact_column = df5.apply(success_impact_function, axis=1)
    df5 = df5.assign(success_impact=success_impact_column.values)


    log("QUESTION 5", output_df=df5,
        other=(df5['success_impact'].min(), df5['success_impact'].max(), df5['success_impact'].mean()))
    return df5


def question_6(df5):
    """
    :param df5: the dataframe created in question 5
    :return: df6
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################
    # Normalize the " popularity " column by scaling between 0 to 100.
    # The least popular movie should be 0 and the most popular one must be 100. It is a float number.
    max_value = df5["popularity"].max()
    min_value = df5["popularity"].min()

    df6 = df5
    # https://stats.stackexchange.com/questions/281162/scale-a-number-between-a-range/281164
    df6["popularity"] = df6["popularity"].apply(lambda val: ((val - min_value)/(max_value-min_value))*(100-0)+0)

    log("QUESTION 6", output_df=df6, other=(df6['popularity'].min(), df6['popularity'].max(), df6['popularity'].mean()))
    return df6


def question_7(df6):
    """
    :param df6: the dataframe created in question 6
    :return: df7
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################
    df7 = df6
    # Change the data type of the "popularity" column to (int16).
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html
    df7 = df7.astype({'popularity': 'int16'})

    log("QUESTION 7", output_df=df7, other=df7['popularity'].dtype)
    return df7


def question_8(df7):
    """
    :param df7: the dataframe created in question 7
    :return: df8
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################
    # Clean the "cast" column by converting the complex value (JSONs) to a comma separated value.
    # The cleaned "cast" column should be a comma-separated value of alphabetically sorted characters
    # (e.g., Angela, Athena, Betty, Chester Rush ) .
    # NOTE: keep unusual characters e.g., '(uncredited)' as they are; no need for further cleansing.

    df8 = df7

    # Fix doubled quotes
    # 'character': 'Red Four (John ""D"")'
    # 'character': ""Bubba's Great Grandmother""
    df8["cast"] = df8["cast"].replace(to_replace=r"\"\"", value=r"\"", regex=True)

    # Fix intext single quoted word like:  'name': "Steve 'Spaz' Williams",
    df8["cast"] = df8["cast"].replace(to_replace=r"(?<=[\w\s])\'(\w+)\'(?=[\w\s])", value=r"INTEXT_SINGLEQUOTE\1INTEXT_SINGLEQUOTE", regex=True)
    # Fix intext double quoted word like:  'character': 'Obi-Wan "Ben" Kenobi',
    df8["cast"] = df8["cast"].replace(to_replace=r"(?<=[\w\s])\"(\w+)\"(?=[\w\s()])",
                                      value=r"INTEXT_DOUBLEQUOTE\1INTEXT_DOUBLEQUOTE", regex=True)
    # Fix intext single quote like:  'character': "Leia's Rebel Escort (uncredited)",
    df8["cast"] = df8["cast"].replace(to_replace=r"(?<=\w)'(?=\w)", value="INTEXT_SINGLEQUOTE", regex=True)

    df8["cast"] = df8["cast"].apply(cast_characters_json_to_sorted_csv)

    log("QUESTION 8", output_df=df8, other=df8["cast"].head(10).values)
    return df8


def question_9(df8):
    """
    :param df9: the dataframe created in question 8
    :return: movies
            Data Type: List of strings (movie titles)
            Please read the assignment specs to know how to create the output
    """

    #################################################
    # Your code goes here ...
    #################################################

    log("QUESTION 9", output_df=None, other=movies)
    return movies


def question_10(df8):
    """
    :param df8: the dataframe created in question 8
    :return: df10
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################

    log("QUESTION 10", output_df=df10, other=df10["release_date"].head(5).to_string().replace("\n", " "))
    return df10


def question_11(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################

    plt.savefig("{}-Q11.png".format(studentid))


def question_12(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################

    plt.savefig("{}-Q12.png".format(studentid))


def question_13(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################

    plt.savefig("{}-Q13.png".format(studentid))


if __name__ == "__main__":
    # TODO Restore
    df1 = question_1("movies.csv", "credits.csv")
    #df1 = question_1("mov1.csv", "cred1.csv")
    df2 = question_2(df1)
    df3 = question_3(df2)
    df4 = question_4(df3)
    df5 = question_5(df4)
    df6 = question_6(df5)
    df7 = question_7(df6)
    df8 = question_8(df7)
    # movies = question_9(df8)
    # df10 = question_10(df8)
    # question_11(df10)
    # question_12(df10)
    # question_13(df10)
