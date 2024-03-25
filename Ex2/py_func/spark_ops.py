import pyspark.sql.functions as F

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window


class sparkTest(object):

    def __init__(
        self,
        data_path: str,
    ):
        self.sp = SparkSession.builder.getOrCreate()
        self.data_path = data_path
        self.src_df = self.get_source_df()
    

    def get_source_df(self) -> DataFrame:

        return self.sp.read\
            .option("escapeQuotes", "true")\
            .option("multiLine", "true")\
            .csv(
                'csv/2020-03-29 Coronavirus Tweets.CSV', 
                header=True,
                quote='"',
                escape='\"',
            )\
            .withColumn(
                "tweet_date", 
                F.regexp_extract(F.input_file_name(), "/(\d{4}-\d{2}-\d{2})", 1)
            )\
            .withColumn('hashtags', F.expr(r"regexp_extract_all(text, '(#\\w+)', 0)") )


    def derived_df_q1(self) -> DataFrame:
        
        derived_df1 = self.src_df.select(
            "tweet_date",
            "user_id",
            "hashtags",
            "source",
            "screen_name",
            "created_at",
        ).withColumn(
            "screen_name", 
            F.last("screen_name")\
                .over(
                    Window\
                        .partitionBy("user_id", "tweet_date")\
                        .orderBy(F.col("created_at").desc())
                )
            )
        
        derived_df1 = derived_df1.groupBy(
            "tweet_date",
            "user_id",
        ).agg(
            F.count("*").alias("num_tweets"),
            F.max("screen_name").alias("screen_name"),
            F.collect_list("source").alias("source_list"),
            F.collect_list("hashtags").alias("hashtags"),
        ).withColumn(
            "source_count", 
            F.size("source_list")
        ).withColumn(
            "tweet_sources", 
            F.map_from_entries(
                F.expr("transform(array_distinct(source_list), x -> struct(x, size(filter(source_list, y -> y = x))))")
            )
        ).withColumn(
            "hashtags",
            F.explode("hashtags")
        )

        return derived_df1.select(
            "tweet_date",
            "user_id",
            "num_tweets",
            "hashtags",
            "tweet_sources",
            "screen_name",
        )


    def derived_df_q2(self) -> DataFrame:

        derived_df2_quote = self.src_df\
            .filter(F.col("is_quote") == "TRUE")\
            .select(
                "tweet_date",
                "user_id",
                "reply_to_user_id",
                "reply_to_status_id",
                "created_at",
            )\
            .withColumnRenamed("user_id", "reply_user_id")\
            .withColumnRenamed("tweet_date", "reply_date")\
            .withColumnRenamed("reply_to_user_id", "original_user_id")\
            .withColumnRenamed("created_at", "reply_at")

        derived_df2 = derived_df2_quote.alias("df1").join(
            self.src_df.select(
                "user_id",
                "created_at",
                "status_id",
            ).alias("df2"),
            F.col("df1.reply_to_status_id") == F.col("df2.status_id"),
            "left",
        ).withColumn(
            'reply_delay',
            F.when(
                F.col("status_id").isNotNull(),
                F.col("reply_at").cast("timestamp") - F.col("created_at").cast("timestamp"),
            )
        ).withColumn(
            'tweet_number', 
            F.when(
                F.col("status_id").isNotNull(), 
                F.expr("row_number() OVER (PARTITION BY reply_to_status_id ORDER BY reply_at)")
            )
        )

        return derived_df2\
            .filter(F.col('status_id').isNotNull())\
            .select(
                "reply_date",
                "reply_user_id",
                "original_user_id",
                "reply_delay",
                "tweet_number"
            )

    def derived_df_q3(self):

        derived_df3 = self.src_df.select(
            "user_id", 
            "screen_name", 
            "created_at"
        ).withColumn(
            "old_screen_name",
            F.lag("screen_name", 1).over(
                Window.partitionBy("user_id").orderBy(F.col("created_at").asc())
            )
        ).filter(
            F.col("old_screen_name") != F.col("screen_name")
        ).withColumn(
            "change_date",
            F.substring("created_at", 1, 10)
        ).drop("created_at")

        return derived_df3
