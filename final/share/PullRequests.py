from pyspark import SQLContext
from pyspark.sql.functions import col, substring
from pyspark.sql import functions as F
from pyspark.sql import DataFrameStatFunctions as statFunc

import statistics
from statistics import mode

def commit_time_analysis(df):
    df2 = df.select("commit_date").withColumn('Hour_of_day_UTC', F.substring("commit_date",12,2))
    df3 = df2.select("Hour_of_day_UTC").groupBy("Hour_of_day_UTC").count()
    df3.write.csv("hdfs://master:9000/output/commit_time_analysis.csv")
    df3.show()

def commit_time(df):
    df2 = df.withColumn('Hour_of_day_UTC', F.substring("commit_date",12,2))
    ct =df2.agg(F.percentile_approx("Hour_of_day_UTC", 0.5).alias("median_hour_of_day"))
    ct.write.csv("hdfs://master:9000/output/median_hour_of_day.csv")
    ct.show()


def mostActiveAuthors(df):
    author = df.groupby(['author_id', 'author_login']).count()
    orderedAuthor =author.sort(col("count").desc())
    orderedAuthor.write.csv("hdfs://master:9000/output/Most_active_authors.csv")

def mostCommonLanguages(df):
    languages = df.groupby(['language']).count()
    orderedlanguages =languages.sort(col("count").desc())
    orderedlanguages.write.csv("hdfs://master:9000/output/Most_common_languages.csv")

def averageCommits(df):
    average = df.agg(F.mean('count(actor_id)'), F.mean('avg(length_of_comment)'))
    #print(average.show())
    average.write.csv("hdfs://master:9000/output/average_actor_commits.csv")

def mostActiveContributers(df):
    contributersCommentLength =df.withColumn('length_of_comment', F.length("comment"))
    c = contributersCommentLength.groupby(['actor_id', 'actor_login']).agg(F.count('actor_id'), F.mean('length_of_comment'))
    orderedContributer =c.sort(col("count(actor_id)").desc(), col("avg(length_of_comment)").asc())
    #print(orderedContributer.show())
    orderedContributer.write.csv("hdfs://master:9000/output/Most_active_actors.csv")
    averageCommits(orderedContributer)


def repos(df):
    df1 = df.withColumn('length_of_comment', F.length("comment"))
    df2 = df1.withColumn('Hour_of_day_UTC', F.substring("commit_date",12,2))
    print(df2.show())
    #r_contributer = df2.groupby(['repo','author_login', 'author_id', 'actor_id']).agg(F.count('actor_id'))
    #print(r_contributer.show())
    r = df2.groupby(['repo','author_login', 'author_id'])\
        .agg(F.countDistinct('actor_id').alias('Distinct_Contributors'),
                                                              F.count('pr_id').alias('Pull_Requests'),
                                                              F.mean('length_of_comment'),
                                                              F.first('language'),
                                                              #F.collect_list('actor_id'),
                                                              #F.collect_list('language').alias('Languages_used'),
                                                              #F.sumDistinct('language').alias('Languages_used'),
                                                              F.percentile_approx("Hour_of_day_UTC", 0.5).alias("median_hour_of_day"))
    #print(r.show())
    rOrdered = r.sort(col("Pull_Requests").desc())
    rOrdered.write.csv("hdfs://master:9000/output/Repo_PR.csv")
    #r = df2.groupby('repo').agg(F.count('actor_id'), F.mean('length_of_comment'))

def nonValidlines(df):
    #df.filter(df.actor_id.rlike('^[a-zA-Z]+$'))
    # df.filter(lambda row: not row[1].isalpha())
    df1 =df.limit(1000)
    df1.write.csv("hdfs://master:9000/output/test15.csv")

if __name__ == '__main__':
    # import findspark

    # findspark.init()

    # # 👇️ /home/borislav/Desktop/bobbyhadz_python/venv/lib/python3.10/site-packages/pyspark
    # print(findspark.find())

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("Workflow").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    sqlContext = SQLContext(spark)
    # df = spark.read.csv(r"/opt/share/ghtorrent-2019-03-11.csv", inferSchema= True, escape = '"')
    df1 = spark.read.csv(r"hdfs://master:9000/ghtorrent-2019-03-11.csv", escape = '"').toDF('actor_login','actor_id','comment_id','comment','repo','language','author_login','author_id','pr_id','c_id','commit_date')
    df2 = spark.read.csv(r"hdfs://master:9000/ghtorrent-2019-03-11.csv", escape = '"').toDF('actor_login','actor_id','comment_id','comment','repo','language','author_login','author_id','pr_id','c_id','commit_date')
    df = df1.union(df2)
    #nonValidlines(df)
    #print(df)

    commit_time_analysis(df)
    commit_time(df)
    mostActiveAuthors(df)
    mostActiveContributers(df)
    repos(df)
    mostCommonLanguages(df)
