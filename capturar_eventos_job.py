import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username='BIMCMFF6WU3YBB34'   password='Xnr9geulvxPYeyNeL2r56iyjNG5dwkB2CTnQz+syVZwOUfJIQFxmSJT0+MskxOnQ';") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("subscribe", "processamento-ted") \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .load()
    
df.selectExpr("CAST(key AS STRING)", "CAST(value AS BINARY)").show()

job.commit()