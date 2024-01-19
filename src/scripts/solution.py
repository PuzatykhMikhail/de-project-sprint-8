import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType
from Config import kafka_security_options, docker_postgresql_settings, postgresql_settings, spark_jars_packages, TOPIC_IN, TOPIC_OUT

# time current
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))


# write to Kafka and PGDB
def foreach_batch_function(df):
    
        df.persist()

        feedback_df = df.withColumn('feedback', f.lit(None).cast(StringType()))

        #PostgreSQL
        feedback_df.write.format('jdbc').mode('append') \
            .options(**docker_postgresql_settings).save()

        # Kafka
        df_to_stream = (feedback_df
                    .select(f.to_json(f.struct(f.col('*'))).alias('value'))
                    .select('value')
                    )

        # write to Kafka
        df_to_stream.write \
            .format('kafka') \
            .options(**kafka_security_options) \
            .option('topic', TOPIC_OUT) \
            .option('truncate', False) \
            .save()

        df.unpersist()
        
# Spark Session        
def spark_init(Spark_Session_Name) -> SparkSession:

        return (SparkSession
            .builder
            .appName({Spark_Session_Name})
            .config("spark.jars.packages", spark_jars_packages)
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        )


def restaurant_read_stream(spark):
    
        df = spark.readStream \
            .format('kafka') \
            .options(**kafka_security_options) \
            .option('subscribe', TOPIC_IN) \
            .load()

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("console") \
            .outputMode("append") \
            .start() \
            .awaitTermination()

        df_json = df.withColumn('key_str', f.col('key').cast(StringType())).withColumn('value_json', f.col('value').cast(StringType())).drop('key', 'value')

        # for json
        incoming_message_schema = StructType([
            StructField('restaurant_id', StringType(), nullable=True),
            StructField('adv_campaign_id', StringType(), nullable=True),
            StructField('adv_campaign_content', StringType(), nullable=True),
            StructField('adv_campaign_owner', StringType(), nullable=True),
            StructField('adv_campaign_owner_contact', StringType(), nullable=True),
            StructField('adv_campaign_datetime_start', LongType(), nullable=True),
            StructField('adv_campaign_datetime_end', LongType(), nullable=True),
            StructField('datetime_created', LongType(), nullable=True),
        ])

        # filter by time
        df_string = df_json.withColumn('key', f.col('key_str')).withColumn('value', f.from_json(f.col('value_json'), incoming_message_schema)).drop('key_str', 'value_json')

        df_filtered = df_string.select(
            f.col('value.restaurant_id').cast(StringType()).alias('restaurant_id'),
            f.col('value.adv_campaign_id').cast(StringType()).alias('adv_campaign_id'),
            f.col('value.adv_campaign_content').cast(StringType()).alias('adv_campaign_content'),
            f.col('value.adv_campaign_owner').cast(StringType()).alias('adv_campaign_owner'),
            f.col('value.adv_campaign_owner_contact').cast(StringType()).alias('adv_campaign_owner_contact'),
            f.col('value.adv_campaign_datetime_start').cast(LongType()).alias('adv_campaign_datetime_start'),
            f.col('value.adv_campaign_datetime_end').cast(LongType()).alias('adv_campaign_datetime_end'),
            f.col('value.datetime_created').cast(LongType()).alias('datetime_created'),
        ).filter((f.col('adv_campaign_datetime_start') <= current_timestamp_utc) & (f.col('adv_campaign_datetime_end') > current_timestamp_utc))
        
        return df_filtered
    
 #Deduplication   
def subscribers_restaurants(spark):

        df = spark.read \
            .format('jdbc') \
            .options(**postgresql_settings) \
            .load()
            
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("console") \
            .outputMode("append") \
            .start() \
            .awaitTermination()

        df = df.dropDuplicates(['client_id', 'restaurant_id'])
        
        return df


def join(restaurant_read_stream_df, subscribers_restaurant_df):
    df = restaurant_read_stream_df.join(subscribers_restaurant_df, 'restaurant_id').withColumn('trigger_datetime_created', f.lit(current_timestamp_utc)) \
        .select(
        'restaurant_id',
        'adv_campaign_id',
        'adv_campaign_content',
        'adv_campaign_owner',
        'adv_campaign_owner_contact',
        'adv_campaign_datetime_start',
        'adv_campaign_datetime_end',
        'datetime_created',
        'client_id',
        'trigger_datetime_created')
        
    return df



if __name__ == '__main__':
    spark = spark_init("SparkService")
 
    spark.conf.set('spark.sql.streaming.checkpointLocation', 'test_query')
 
    restaurant_read_stream_df = restaurant_read_stream(spark)

    subscribers_restaurant_df = subscribers_restaurants(spark)

    result = join(restaurant_read_stream_df, subscribers_restaurant_df)

    query = (result.writeStream.foreachBatch(foreach_batch_function).start())
    
    try:
        query.awaitTermination()
    finally:
        query.stop()
        




