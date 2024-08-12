from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Merge CSV Files") \
    .getOrCreate()

# Read the first CSV file
csv_file1 = './amazon_goodread.csv'
df1 = spark.read.csv(csv_file1, header=True, inferSchema=True)

# Read the second CSV file
csv_file2 = './wonderbk.csv'
df2 = spark.read.csv(csv_file2, header=True, inferSchema=True)

# Perform an inner join on the 'isbn' column
merged_df = df1.join(df2, on='isbn', how='inner').select(df1["isbn"])

# Show the result
merged_df.show()

# Optionally, you can save the merged DataFrame to a new CSV file
output_file = './merged_output.csv'
merged_df.write.csv(output_file, header=False)
