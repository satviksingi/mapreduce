
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, count, col, regexp_extract

spark = SparkSession.builder.appName("WordCount") .getOrCreate()

file_path = spark.read.text(r"/Users/satvikreddysingireddy/Downloads/file1.txt")
text_word = file_path.select(explode(split(file_path.value, " ")).alias("Word"))

text_word = text_word.filter(regexp_extract("Word", r'^[a-zA-Z]{1}$|^[a-zA-Z]+$', 0) != '')

word_count = text_word.groupBy("Word").agg(count("*").alias("Count"))

word_count.show(word_count.count(), truncate=False)

spark.stop()