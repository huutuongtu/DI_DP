from pyspark.sql import SparkSession
from pyspark.sql.functions import col, levenshtein, length, lower, regexp_replace
import time

# Initialize Spark session
spark = SparkSession.builder.appName("LevenshteinJoin").getOrCreate()

# # Sample DataFrames
# data1 = [("1", "Harry Potter and the Philosopher's Stone"), ("2", "The Hobbit"), ("3", "The Catcher in the Rye")]
# data2 = [("a", "Harry Potter and the Sorcerer's Stone"), ("b", "Hobbit"), ("c", "Catcher in the Rye")]

# columns = ["id", "book_title"]
csv_file1 = '/home/jovyan/voice-chung/tuht/DI_DP/amazon_goodread.csv'
csv_file2 = '/home/jovyan/voice-chung/tuht/DI_DP/wonderbk_new.csv'

df1 = spark.read.csv(csv_file1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file2, header=True, inferSchema=True)

# df1 = df1.sample(fraction=0.01)
# df2 = df2.sample(fraction=0.01)

df2 = df2.selectExpr(
    "book_title as book_title_2",
)

df1 = df1.withColumn("norm_book_title", lower(regexp_replace(col("book_title"), "\\s+", "")))
df2 = df2.withColumn("norm_book_title_2", lower(regexp_replace(col("book_title_2"), "\\s+", "")))

# Join condition with Levenshtein distance and CER threshold
cer_threshold = 0.1

# Add a cross join and filter based on the CER threshold
joined_df = df1.crossJoin(df2).filter(
    (levenshtein(col("norm_book_title"), col("norm_book_title_2")) / length(col("norm_book_title"))) < cer_threshold
)

# Select the relevant columns
result_df = joined_df.select(col("book_title"),
                             col("norm_book_title"),
                             col("book_title_2"),
                             col("norm_book_title_2")
                             )

#8s for 1% df1 and 1% df2 data 
#80s for 10% df1 and 1% df2 data
# if we use spark on all dataset: nearly 1 day (about 22 hours), if we use python, it cost 1 month
start = time.time()
# Show the result
result_df.show(truncate=False)
end = time.time()
print("time processing")
print(end-start)

result_df.write.csv('./merge.csv', header=True)
# Stop the Spark session
spark.stop()
