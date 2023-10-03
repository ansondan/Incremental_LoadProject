
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("fraud project") \
    .master("local") \
    .enableHiveSupport().getOrCreate()

dburl="jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"

max = spark.sql("select max(row_id) from ansong.fraud_data as max")

max = max.first()['max(row_id)']

query="(select * from frauddetection_fullsample where row_id >"+str(max)+ ") as tb"
df = spark.read.format("jdbc").option("url",dburl) \
    .option("driver", "org.postgresql.Driver").option("dbtable", query) \
    .option("user", "consultants").option("password", "WelcomeItc@2022").load()

print(df.show())

df = df.withColumnRenamed("type", "transaction_type")

# Create Hive Internal table
df.write.mode('append').saveAsTable("ansong.fraud_data")

spark.stop()
