# Databricks notebook source
# Required data types 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType, LongType, FloatType, MapType, ArrayType

# COMMAND ----------

#Defining the schema
movies_metadata_schema = StructType([
                                    StructField("adult", BooleanType(),True),
                                StructField("belongs_to_collection", StringType(),True),
                                StructField("budget", IntegerType(),True),
                                StructField("genres", StringType(),True),
                                StructField("homepage", StringType(),True),
                                StructField("id", IntegerType(),True),
                                StructField("imdb_id", StringType(),True),
                                StructField("original_language", StringType(),True),
                                StructField("original_title", StringType(),True),
                                StructField("overview", StringType(),True),
                                StructField("popularity", FloatType(),True),
                                StructField("poster_path", StringType(),True),
                                StructField("production_companies", StringType(),True),
                                StructField("production_countries", StringType(),True),
                                StructField("release_date", StringType(),True),
                                StructField("revenue", LongType(),True),
                                StructField("runtime", DoubleType(),True),
                                StructField("spoken_languages", StringType(),True),
                                StructField("status", StringType()),
                                StructField("tagline", StringType(),True),
                                StructField("title", StringType(),True),
                                StructField("video", BooleanType(),True),
                                StructField("vote_average", DoubleType(),True),
                                StructField("vote_count", IntegerType(),True), 
                            ])

# COMMAND ----------

# Reading the Movies_metadata.csv File

movies_metadata_df = spark.read.option("header", True)\
                                .option("multiline",True)\
                                .option("escape", "\"")\
                                .schema(movies_metadata_schema)\
                                .csv("/mnt/moviegoers/raw/movies_metadata.csv")

# COMMAND ----------

#Renaming the columns to avoid conlicts with JSON data 

rc_movies_metadata=movies_metadata_df.withColumnRenamed("id","id_original")\
                                .withColumnRenamed("poster_path","poster_path_original")

# COMMAND ----------

#dropping rows for null values if identified in the specified columns
Nulldrop_movies_metadata= rc_movies_metadata.dropna(subset=("production_companies","production_countries"))

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import when,col,from_json,explode,map_keys,count,isnan

# COMMAND ----------

#Replace empty [] to avoid Json parsing issues  
movies_metadata_json=Nulldrop_movies_metadata.withColumn('genres',when(col('genres')=='[]',"[{'id': 0, 'name': 'Unknown'}]").otherwise(col('genres')))\
                               .withColumn('production_companies',when(col('production_companies')=='[]',"[{'name': 'Unknown', 'id': 0}]").otherwise(col('production_companies')))\
                               .withColumn('production_countries',when(col('production_countries')=='[]',"[{'iso_3166_1': 'Unknown', 'name': 'Unknown'}]").otherwise(col('production_countries')))


# COMMAND ----------

# Null Count Check
datadict={'Movies_metadata.csv':movies_metadata_json}

for key,value in datadict.items() :
  dfStats= value.select([count(when(col(c).isNull()|isnan(c),'True')).alias(c) for c,c_type in value.dtypes if c_type not in ('timestamp','boolean')])
  print("Column stats for data file :" +key+"\n")
  dfStats.show()

# COMMAND ----------

#Json Column Parsing: belongs_to_collection_value column

#1 Schema
df=movies_metadata_json.withColumn("belongs_to_collection_value",from_json(movies_metadata_json.belongs_to_collection,MapType(StringType(),StringType())))


# COMMAND ----------

#Selecting distinct values
key_df=df.select(explode(map_keys(col('belongs_to_collection_value')))).distinct()


# COMMAND ----------

#converting key collection objects to List 
keylst=list(map(lambda row:row[0],key_df.collect()))


# COMMAND ----------

#Retriving the values based on the Keys into a seperate column

key_cols=map(lambda f:df['belongs_to_collection_value'].getItem(f).alias(str(f)),keylst)

# COMMAND ----------

df=df.select(*movies_metadata_json.columns,*key_cols)

# COMMAND ----------

# production_countries , production_companies and genres have Json array values
#Step1 - Definin the  schema of Json array type
schema = ArrayType(StructType([
        StructField('id', IntegerType(), nullable=False), 
        StructField('name', StringType(), nullable=False)]))


# COMMAND ----------

#Step 2 - Using the UDF function to convert list to column seperated values. Extracting values from Json array based on Json keys inorder to produce the list
convertUDF = udf(lambda s: ','.join(map(str, s)),StringType())

# COMMAND ----------

#3 Parsing Json in the columns
df=df.withColumn("production_companies_values",when(col('production_companies')=='[]','').otherwise(convertUDF(from_json(movies_metadata_json.production_companies,schema).getField("name"))))\
     .withColumn("production_countries_values",convertUDF(from_json(movies_metadata_json.production_countries,schema).getField("name")))\
     .withColumn("genres_value",convertUDF(from_json(movies_metadata_json.genres,schema).getField("name")))


# COMMAND ----------

 # selecting only two columns for verification purpose. Keep adding all the columns in the csv file which are required for reports
  finaldf = df.select("production_companies_values","Production_countries_values")

# COMMAND ----------

#Verify the columns are displaying the correct information
display(finaldf)

# COMMAND ----------

#write to the processed location as Parquet
finaldf.write.mode("overwrite").parquet("/mnt/moviegoers/processed/metadata")

# COMMAND ----------

#Work in progress:
#Issue: Handle Null Values when writing to Parquet from Spark.
#Resolution: Get dataframe schema
my_schema = list(df.schema)

null_cols = []

# iterate over schema list to filter for NullType columns
for st in my_schema:
    if str(st.dataType) == 'NullType':
        null_cols.append(st)

# cast null type columns to string (or whatever you'd like)
for ncol in null_cols:
    mycolname = str(ncol.name)
    df = df \
        .withColumn(mycolname, df[mycolname].cast('string'))
