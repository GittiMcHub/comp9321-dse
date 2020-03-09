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


def extract_field_from_json(json_str: str, field: str):
    json_str = json_str.replace("'", "\"")
    json_array = json.loads(json_str)
    generic_list = []

    for key in json_array:
        generic_list.append(key[field])
    return generic_list


def cast_characters_json_to_sorted_csv(json_str: str):
    # Convert to real json:  'character' => "character"
    json_str = json_str.replace("'", "\"")
    # Replace placeholders
    json_str = json_str.replace("INTEXT_DOUBLEQUOTE", "\\\"")
    json_str = json_str.replace("INTEXT_SINGLEQUOTE", "'")
    json_str = json_str.replace("None", "\"null\"")
    # print(json_str)

    json_array = json.loads(json_str)
    char_list = []
    for char_json in json_array:
        char_list.append(char_json["character"])
    return ", ".join(sorted(char_list))


def characters_string_to_char_count(row: str):
    return len(list(set(row.split(","))))


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

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.replace.html

    df8 = df7

    """The following regex collection will handle these cases:
        
        Not JSON Conform 
         'character': 'Mos Eisley Citizen (special edition)',
         
        Double Quoted JSON value
         'character': ""Mickey Mouse (segment 'The Sorcerer's Apprentice') (voice)"",
         'name': "Steve 'Spaz' Williams",
         'character': ""Pianist in 'El Rancho' (uncredited)"",
         
        Single Quote in Text or in double quoted JSON Value
         'character': "Leia's Rebel Escort (uncredited)",
         'character': ""Rocky Jr.'s Friend"",
         'character': ""Four Seasons Maître d'"",
         'character': ""Member of Angel Eyes' Gange""
         'character': ""'60s Model"",
         'character': ""Bubba's Great Grandmother"",
         'character': ""'Fat' Moe Gelly"",
         'character': ""Little Jack 'L.J.' Byrnes""
         'character': ""Schwester Mary Stigmata ('Die Pinguin-Tante')"",
         'character': ""Fight Patron Saying 'I don't know. What's going on?'"",
         'character': ""Floyd 'D'"",
         
        Double Quotes in Text
         'character': 'Obi-Wan "Ben" Kenobi',
         
        Double double Quotes in Text
         'character': 'Red Four (John ""D"")'
         'character': '""The Blonde"" in T-Bird',
         'character': 'Georg ""Schorsi""',
         'character': 'Alastor ""Mad-Eye"" Moody',
         
        Escaping Characters 
         'character': ""Alastor 'Mad\xadEye' Moody"",
         'character': 'Old Man Getting Umbrella in ""Singin\' in the Rain"" Number (uncredited)',
    """

    # Fix doubled quotes
    # 'character': 'Red Four (John ""D"")'
    # 'character': ""Bubba's Great Grandmother""
    df8["cast"] = df8["cast"].replace(to_replace=r"\"\"", value=r"\"", regex=True)

    # Fix this monster:  'character': 'Old Man Getting Umbrella in ""Singin\' in the Rain"" Number (uncredited)',
    df8["cast"] = df8["cast"].replace(to_replace=r"\\'", value=r"'", regex=True)

    # Fix a single backslash like 'character': ""Alastor 'Mad\xadEye' Moody"",
    df8["cast"] = df8["cast"].replace(to_replace=r"\\", value=r"\\\\", regex=True)

    # Fix intext single quoted word like:  'name': "Steve 'Spaz' Williams",
    df8["cast"] = df8["cast"].replace(to_replace=r"(?<=[\w\s\"(])\'([\w\s\\?\.-]+)\'(?=[\w\s\")])",
                                      value=r"INTEXT_SINGLEQUOTE\1INTEXT_SINGLEQUOTE", regex=True)

    # Fix intext double quoted word like:  'character': 'Obi-Wan "Ben" Kenobi',
    df8["cast"] = df8["cast"].replace(to_replace=r"(?<=[\w\s'])\"([\w\s'-]+)\"(?=[\w\s()'])",
                                      value=r"INTEXT_DOUBLEQUOTE\1INTEXT_DOUBLEQUOTE", regex=True)

    # Fix intext single quote like:  'character': "Leia's Rebel Escort (uncredited)",
    df8["cast"] = df8["cast"].replace(to_replace=r"(?<=[\w\"\.])'(?=[\w\"\s)])", value="INTEXT_SINGLEQUOTE", regex=True)

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
    # Return a list, containing the names of the top 10 movies according to the number of movie characters
    # (Harry Potter! is one character! do not count the letters in the title of movies!).
    # The first element in the list should be the movie with the most number of characters.

    df8["char_count"] = df8["cast"].apply(characters_string_to_char_count)
    top10df = df8.nlargest(10, "char_count", keep='first')
    # print(top10df.head(n=15))
    movies = top10df["title"].tolist()

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
    # Sort the dataframe by the release date
    # (the most recently released movie should be first row in the dataframe)

    # print(df8["release_date"].dtype)

    df10 = df8
    df10["release_date"] = pd.to_datetime(df10["release_date"])
    df10 = df10.sort_values("release_date", axis=0, ascending=False)

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
    # Plot a pie chart, showing the distribution of genres in the dataset (e.g., Family, Drama).
    # Show the percentage of each genre in the pie chart.
    df11 = df10
    df11["genre_list"] = df11["genres"].apply(lambda col_val: extract_field_from_json(col_val, "name"))

    # print(df11["genre_list"].head())
    genre_dict = {}

    for index, row in df11.iterrows():
        for genre in row["genre_list"]:
            if genre in genre_dict:
                old_val = genre_dict[genre]
                old_val = old_val + 1
                genre_dict.update({genre: old_val})
            else:
                genre_dict.update({genre: 1})

    # Reduce overlapping
    plt.rcParams.update({'font.size': 6})

    # https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.pie.html
    # https://pyformat.info/
    plt.pie(genre_dict.values(), labels=genre_dict.keys(), autopct='%1.1f%%', pctdistance=1.2, labeldistance=1.3)
    # plt.show()

    plt.savefig("{}-Q11.png".format(studentid))


def question_12(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################
    # Plot a bar chart of the countries in which movies have been produced.
    # For each county you need to show the count of movies.
    # Countries should be alphabetically sorted according to their names.
    df11 = df10
    df11["country_list"] = df11["production_countries"].apply(lambda col_val: extract_field_from_json(col_val, "name"))

    country_dict = {}

    for index, row in df11.iterrows():
        for genre in row["country_list"]:
            if genre in country_dict:
                old_val = country_dict[genre]
                old_val = old_val + 1
                country_dict.update({genre: old_val})
            else:
                country_dict.update({genre: 1})

    sorted_values = []
    for country in sorted(country_dict.keys()):
        sorted_values.append(country_dict[country])

    # https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.bar.html
    plt.clf()
    plt.rcParams.update({'font.size': 10})
    plt.figure(figsize=(15, 10))
    plt.xticks(rotation=90)
    plt.bar(sorted(country_dict.keys()), sorted_values)
    # https://stackoverflow.com/questions/10101700/moving-matplotlib-legend-outside-of-the-axis-makes-it-cutoff-by-the-figure-box
    plt.subplots_adjust(bottom=0.2)
    # plt.show()
    plt.savefig("{}-Q12.png".format(studentid))


def question_13(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################
    # - (1.5 Marks) Plot a scatter chart with x axis being "vote_average" and y axis being "success_impact".
    # - (0.5 Marks) Ink bubbles based on the movie language (e.g, English, French);
    #   In case of having multiple languages for the same movie, you are free to pick any one as you wish.
    # - (0.5 Marks) Add a legend showing the name of languages and their associated colors.
    df11 = df10

    # Switching to original_language to avoid font display issues
    # Allowed from Mohammadali Yaghoubzadehfard
    # Mon Mar 09 2020 14:36:11 GMT+1100 (Ostaustralische Sommerzeit)

    # Extract the languages given in the json
    #df11["language_list"] = df11["spoken_languages"].apply(lambda col_val: extract_field_from_json(col_val, "name"))
    # Method: choose first
    #df11["language_selected"] = df11["language_list"].apply(lambda col_list: col_list[0])

    # Group by language for color map
    groups = df11[["original_language", "vote_average", "success_impact"]].groupby("original_language").median()
    # Manual Color Map
    color_map = ["#000000", "#1f77b4", "#d61515", "#1ba11b", "#ff7f0e", "#8a53bd", "#bcbd22", "#17becf", "#7f7f7f", "#aec7e8", "#ff9896", "#98df8a", "#ffbb78", "#dbdb8d", "#ff00ff"]
    # Map language to an index of the color map
    language_to_color_map = {}
    map_counter = 0
    for index, row in groups.iterrows():
        if index not in language_to_color_map:
            language_to_color_map.update({index: map_counter})
            map_counter = map_counter + 1
            if map_counter == len(color_map):
                map_counter = 1

    # Plot Scatter chart

    plt.clf()
    plt.figure(figsize=(20, 25))
    # https://matplotlib.org/3.1.0/tutorials/text/text_props.html#text-with-non-latin-glyphs
    # plt.rcParams['font.family'] = 'Open Sans'

    # https://matplotlib.org/3.1.1/gallery/lines_bars_and_markers/scatter_with_legend.html
    fig, ax = plt.subplots()
    for index, row in df11.iterrows():
        x, y, = row["vote_average"], row["success_impact"]
        label, color = row["original_language"], color_map[language_to_color_map[row["original_language"]]]

        ax.scatter(x, y, c=color, s=4.0, label=label, edgecolors='face')

    # Set Labels
    plt.title("vote_average vs. success_impact")
    plt.xlabel("vote_average")
    plt.ylabel("success_impact")

    # Now "English" is listed many times, we have to reduce the handles to show distinct values
    # There must be a proper way but I couldn't find it...
    handles, labels = ax.get_legend_handles_labels()
    indices = []
    new_labels = []
    for idx in range(len(labels)):
        if labels[idx] not in new_labels:
            indices.append(idx)
            new_labels.append(labels[idx])

    new_handles = []
    for idx in indices:
        new_handles.append(handles[idx])

    # https://matplotlib.org/tutorials/intermediate/legend_guide.html
    ax.legend(new_handles, new_labels, bbox_to_anchor=(0, -1.2, 1, 1), loc='upper center',
           ncol=4, mode="expand", frameon=True)

    ax.grid(True)
    # https://stackoverflow.com/questions/10101700/moving-matplotlib-legend-outside-of-the-axis-makes-it-cutoff-by-the-figure-box
    fig.subplots_adjust(bottom=0.5)

    # plt.show()

    plt.savefig("{}-Q13.png".format(studentid))


if __name__ == "__main__":
    df1 = question_1("movies.csv", "credits.csv")
    df2 = question_2(df1)
    df3 = question_3(df2)
    df4 = question_4(df3)
    df5 = question_5(df4)
    df6 = question_6(df5)
    df7 = question_7(df6)
    df8 = question_8(df7)
    movies = question_9(df8)
    df10 = question_10(df8)
    question_11(df10)
    question_12(df10)
    question_13(df10)
