
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("FullLoad Fraud") \
    .master("local") \
    .enableHiveSupport().getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
    .option("dbtable", "frauddetection_sample") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()

df = df.withColumnRenamed("type", "transaction_type")

df.write.mode('overrite').saveAsTable("ansong.fraudtable")

spark.stop()