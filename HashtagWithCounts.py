import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.functions import to_utc_timestamp
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
from pyspark.sql.functions import window
from datetime import datetime
import pytz


window_length = '50 seconds'
sliding_interval = '50 seconds'

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: spark-submit m02_demo09_countHashtags.py <hostname> <port>", 
                file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession\
        .builder\
        .appName("HashtagCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    lines = spark\
        .readStream\
        .format("socket")\
        .option("host", host)\
        .option("port", port)\
        .load()

    schema = StructType().add('text', StringType(), False).add('created_at', StringType(), False)
    df = lines.selectExpr('CAST(value AS STRING)').select(from_json('value', schema).alias('temp')).select('temp.*')

    def extract_tags(word):
        if word.lower().startswith("#"):
            return word
        else:
            return "nonTag"

    ## Converting date string format
    def getDate(x):
        if x is not None:
            return str(datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"))

    date_fn = udf(getDate, StringType())

    ## Converting datatype in spark dataframe
    df = df.withColumn("created_at", to_utc_timestamp(date_fn("created_at"), "UTC")) 
    
    words = df.select(
        explode(
            split(df.text, " ")
        ).alias("word"), df.created_at.alias("timestamp")
    )
    extract_tags_udf = udf(extract_tags, StringType())
    resultDF = words.withColumn("tags", extract_tags_udf(words.word))\
                    .withColumn("timestamp", words.timestamp)


    windowedCounts = resultDF.where(resultDF.tags != "nonTag").groupBy(
        window(words.timestamp, window_length, sliding_interval),
        words.word).count().orderBy('window')

    query = windowedCounts.writeStream\
                      .outputMode("complete")\
                      .format("console")\
                      .option("truncate", "false")\
                      .start()\
                      .awaitTermination()
