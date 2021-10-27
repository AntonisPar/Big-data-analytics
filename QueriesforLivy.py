from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import from_unixtime, hour, dayofyear
from pyspark.sql.functions import isnull, when, count, col
from pyspark.sql.functions import rank, col
from pyspark.sql.functions import *
from pyspark.sql import Window
import pyspark.sql.functions as f
import pyspark.sql.functions as F


movies = (spark.read
      .format("csv")
      .option('header', 'true') #means that the first line contains column names
      .option("delimiter", ",") #set the delimiter to comma
      .option("inferSchema", "true") #automatically try to infer the column data types
      .load("/home/administrator/Downloads/movielens/movie.csv") #filename to read from
     )
     
ratings = (spark.read
      .format("csv")
      .option('header', 'true') #means that the first line contains column names
      .option("delimiter", ",") #set the delimiter to comma
      .option("inferSchema", "true") #automatically try to infer the column data types
      .load("/home/administrator/Downloads/movielens/rating.csv") #filename to read from
     )

tag = (spark.read
      .format("csv")
      .option('header', 'true') #means that the first line contains column names
      .option("delimiter", ",") #set the delimiter to comma
      .option("inferSchema", "true") #automatically try to infer the column data types
      .load("/home/administrator/Downloads/movielens/tag.csv") #filename to read from
     )

#QUERY 1



merged = movies.join(ratings, on=['movieId'], how='left_outer')

seen_jumanji = merged\
.filter(F.col("title") == "Jumanji (1995)").count()

print(seen_jumanji)


#QUERY 2


tags_and_movies= movies.join(tag, on=['movieId'], how='left_outer')

boring = tags_and_movies\
.select('title')\
.where(tags_and_movies['tag'].contains("boring")).dropDuplicates()

boring = boring\
.orderBy(col('title'))

boring.show()


#QUERY 3

tags_and_ratings = ratings.join(tag,on=['userId'], how='left_outer')


bollywood= tags_and_ratings\
.select('userId')\
.where((F.col('tag').like("%bollywood") | F.col('tag').like("%BOLLYWOOD") | F.col('tag').like("%Bollywood")) & (F.col('rating') > 3)).distinct()

bollywood.sort('userId')
          
bollywood.show()


#QUERY 4


query4 = ratings.join(movies,on=['movieId'], how ='left_outer')

top_rated = query4\
.groupBy("title",ratings['timestamp'].substr(1,4).alias("Year"))\
.agg(avg(col("rating")))\
.withColumnRenamed("avg(rating)", "avg_rating")\
.sort(desc("avg_rating"))



def get_topN(df, group_by_columns, order_by_column, n=10):
    window_group_by_columns = Window.partitionBy(group_by_columns)
    ordered_df = df.select(df.columns + [
        f.row_number().over(window_group_by_columns.orderBy(order_by_column.desc())).alias('row_rank')])
    topN_df = ordered_df.filter(f"row_rank <= {n}").drop("row_rank")
    return topN_df

final_top_rated = get_topN(top_rated, top_rated["Year"],top_rated["avg_rating"], 10)




results = final_top_rated\
.filter(F.col("Year") == "2005")\
.orderBy(F.col("title").asc())


results.show()

#QUERY 5


tags = tag\
.select("movieId","tag")\
.where(col('timestamp').substr(1,4) == '2015')

tags_and_movies = tags.join(movies, on=['movieId'])


tags_and_movies = tags_and_movies\
.sort(col("title").asc())

tags_and_movies.show(50)


#QUERY 6


ratings_count = ratings\
.groupBy("movieId")\
.agg(count("userId"))\
.withColumnRenamed("count(userId)", "num_ratings")\


ratings_per_movie = ratings_count.join(movies, ratings_count.movieId == movies.movieId)


ratings_per_movie = ratings_per_movie\
.sort(desc("num_ratings"))

ratings_per_movie.show(20, truncate=False)


#QUERY 7

most_ratings = ratings\
.groupBy("userId",ratings['timestamp'].substr(1,4).alias("Year"))\
.agg(count(col("rating")))\
.withColumnRenamed("count(rating)", "counts")\
.orderBy(desc("counts"))


def get_topN(df, group_by_columns, order_by_column, n=10):
    window_group_by_columns = Window.partitionBy(group_by_columns)
    ordered_df = df.select(df.columns + [
        f.row_number().over(window_group_by_columns.orderBy(order_by_column.desc())).alias('row_rank')])
    topN_df = ordered_df.filter(f"row_rank <= {n}").drop("row_rank")
    return topN_df

final_most_ratings = get_topN(most_ratings, most_ratings["Year"],most_ratings["counts"], 10)

results = final_most_ratings\
.filter(F.col("Year") == "1995")\
.sort(col('userId').asc())

results.show()


#QUERY 8


movies_and_ratings = movies.join(ratings,on=['movieId'], how='left_outer')


genres = movies_and_ratings.withColumn("genres",explode(split("genres","[|]")))


FinalGenres = genres\
.groupby("genres","title")\
.agg(count(col("rating")))\
.withColumnRenamed("count(rating)", "num_ratings")\
.sort(desc("num_ratings"))

# FinalGenres.show(2000)

TheFinalGenres = FinalGenres\
.groupby("genres","title")\
.agg(max(col("num_ratings")))\
.withColumnRenamed("max(num_ratings)","ratings_num")\
.sort(desc("ratings_num"))

def retrieve_topN(df, group_by_columns, order_by_column, n):
    window_group_by_columns = Window.partitionBy(group_by_columns)
    ordered_df = df.select(df.columns + [
        f.row_number().over(window_group_by_columns.orderBy(order_by_column.desc())).alias('row_rank')])
    topN_df = ordered_df.filter(f"row_rank <= {n}").drop("row_rank")
    return topN_df


final_most_ratings_genres = retrieve_topN(TheFinalGenres, TheFinalGenres["genres"],TheFinalGenres["ratings_num"], 1)

final_most_ratings_genres = final_most_ratings_genres\
.sort(col('genres').asc())


#QUERY 9

df4 = ratings\
.groupby("timestamp","movieId")\
.agg(count(col("userId")))\
.withColumnRenamed("count(userId)", "number of users")\
.filter(F.col("number of users") != 1)\
.sort(desc("number of users"))


df5 = df4\
.select(sum("number of users"))

df5.show()


#QUERY 10

tags_and_ratings = ratings.join(tag,on=["movieId"], how='left_outer')

df1 = tags_and_ratings\
.select("movieId","tag")\
.where((F.col('tag') == "funny") & (F.col('rating') > 3.5)).dropDuplicates()


movies_funny = df1.join(movies,on=["movieId"], how='left_outer')



df2 = movies_funny.withColumn("genres",explode(split("genres","[|]")))

final_df = df2\
.groupby("genres")\
.agg(count(col("title")))\
.withColumnRenamed("count(title)", "number of movies")\
.sort(col("genres").asc())

final_df.show()

