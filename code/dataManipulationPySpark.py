
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('avto parse').getOrCreate()

df = spark.read.csv('../data/scrapResult.txt', inferSchema=True, header=False, sep =";;")

# Split the floor/storeys column into two separate columns
split_column = F.split(df['_c8'], "/")

df = df.withColumn('aptFloor',  F.regexp_replace(split_column.getItem(0) , r' ', '' )) \
  .withColumn('aptStoreys', F.regexp_replace(split_column.getItem(1) , r' ', '' )) \
  .withColumn('aptTotalPrice', F.regexp_replace(F.col('_c1'), r",", '' )) \
  .withColumn('aptPricePerSqm', F.regexp_replace(F.col('_c2'), r",", '' )) \
  .withColumn('aptCeilingHeight', F.regexp_replace(F.col('_c5'), r" M", '' )) \
  .withColumn('aptArea', F.regexp_replace(F.col('_c6'), r" SQ. M.", '' )) \
  .withColumn('aptBldgType', F.lower(F.col('_c4'))) \
  .withColumn('aptCondition', F.lower(F.col('_c9'))) \
  .withColumn('aptStreet', F.lower(F.regexp_replace(F.col('_c10'), r"\+", ' ')))\
  .withColumn('aptDistrict', F.lower(F.col('_c11'))) \
  .withColumn('aptRegion', F.lower(F.col('_c12'))) \
  .withColumn('aptFacilities', F.lower(F.translate(F.col('_c13'), "\ []\'", '')))\
  .withColumn('aptAdditionalFacilities', F.lower(F.regexp_replace(F.col('_c14'), "[\ \[\]\']", '')))\
  .withColumn("aptNewConstruction", F.when(F.col('_c20').contains("-in-new-construction"), 1).otherwise(0)) \
  .withColumnRenamed('_c0', 'aptId') \
  .withColumnRenamed('_c3', 'aptBath') \
  .withColumnRenamed('_c7', 'aptNoOfRooms') \
  .withColumnRenamed('_c15', 'aptViews') \
  .withColumnRenamed('_c16', 'aptAddedDate') \
  .withColumnRenamed('_c17', 'aptEditedDate') \
  .withColumnRenamed('_c18', 'aptLat') \
  .withColumnRenamed('_c19', 'aptLng') \
  .withColumnRenamed('_c20', 'aptURL') \
  .drop('_c1','_c2','_c4','_c5','_c6','_c8','_c9','_c10','_c11','_c12','_c13','_c14')

# Create expression for unpacking the facilities and the additional facilities
facilities = [item for item in df.select("aptFacilities",F.explode(F.split("aptFacilities",","))).select('col').distinct().rdd.flatMap(lambda x: x).collect() if item != '']
additionalFacilities = [item for item in df.select('aptAdditionalFacilities',F.explode(F.split('aptAdditionalFacilities',","))).select('col').distinct().rdd.flatMap(lambda x: x).collect() if item != '']
types_expr = [F.when(F.col("aptFacilities").contains(ty), 1).otherwise(0).alias("aptFacilities_" + ty) for ty in facilities]
codes_expr = [F.when(F.col('aptAdditionalFacilities').contains(code), 1).otherwise(0).alias("aptAdditionalFacilities_" + code) for code in additionalFacilities]
df = df.select(*df.columns, *types_expr+codes_expr)
df.write.csv('../data/result_01.csv', header = True)
