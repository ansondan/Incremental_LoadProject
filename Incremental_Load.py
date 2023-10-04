
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("fraud project") \
    .master("local") \
    .enableHiveSupport().getOrCreate()

dburl="jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"

max = spark.sql("select max(row_id) from ansong.fraud_data2 as max")

max = max.first()['max(row_id)']
max += 1
endmax = max + 10000
query="(select * from frauddetection where row_id between "+str(max)+" and "+str(endmax)+") as tb"
df = spark.read.format("jdbc").option("url",dburl) \
    .option("driver", "org.postgresql.Driver").option("dbtable", query) \
    .option("user", "consultants").option("password", "WelcomeItc@2022").load()

print(df.show())

df = df.withColumnRenamed("type", "transaction_type")

# Create Hive Internal table
df.write.mode('append').format("hive").saveAsTable("ansong.fraud_data2")

spark.stop()
