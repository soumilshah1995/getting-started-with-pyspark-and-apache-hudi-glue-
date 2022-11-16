try:
    import os
    import sys
    import uuid

    import pyspark
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, asc, desc
    from awsglue.utils import getResolvedOptions
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.context import GlueContext

    from faker import Faker

    print("All modules are loaded .....")

except Exceptionx as e:
    print("Some modules are missing {} ".format(e))

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

args = getResolvedOptions(sys.argv, ['base_s3_path', 'table_name'])

base_s3_path = args['base_s3_path']
table_name = args['table_name']


final_base_path = "{base_s3_path}/tmp/{table_name}".format(
    base_s3_path=base_s3_path, table_name=table_name
)

target_s3_path = "{base_s3_path}/tmp/hudi_{table_name}_target".format(
    base_s3_path=base_s3_path,
    table_name=table_name
)

global faker
faker = Faker()


class DataGenerator(object):

    @staticmethod
    def get_data():
        return [
            (
                uuid.uuid4().__str__(),
                faker.name(),
                faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
                faker.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
                faker.random_int(min=10000, max=150000),
                faker.random_int(min=18, max=60),
                faker.random_int(min=0, max=100000),
                faker.unix_time()
            ) for x in range(10)
        ]


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .getOrCreate()
    return spark


spark = create_spark_session()
sc = spark.sparkContext
glueContext = GlueContext(sc)

data = DataGenerator.get_data()
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]

hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': 'emp_id',
    'hoodie.datasource.write.partitionpath.field': 'state',
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
}

# ====================================================
"""Create Spark Data Frame """
# ====================================================
df = spark.createDataFrame(data=data, schema=columns)


# ====================================================
"""Write into HUDI tables """
# ====================================================

df.write.format("hudi").options(
    **hudi_options).mode("overwrite").save(final_base_path)

# ====================================================
"""read from Hudi table"""
# ====================================================
userSnapshotDF = spark.read.format("hudi").load(final_base_path)

userSnapshotDF.createOrReplaceTempView("hudi_users_snapshot")


# ====================================================
"""APPEND """
# ====================================================
impleDataUpd = [
    (3, "Gabriel","Sales","RJ",81000,30,23000,827307999),
    (7, "Paulo","Engineering","RJ",79000,53,15000,1627694678),
]

columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
usr_up_df = spark.createDataFrame(data=data, schema=columns)
usr_up_df.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(final_base_path)
# ====================================================

usr_df_read = spark.read.format("hudi").load(final_base_path)
usr_up_df.createOrReplaceTempView("hudi_users_view")

spark.sql(f"CREATE DATABASE IF NOT EXISTS hudi_demo")
spark.sql(f"DROP TABLE IF EXISTS hudi_demo.users")
spark.sql(f"CREATE TABLE IF NOT EXISTS hudi_demo.users USING PARQUET LOCATION '{final_base_path}' as (SELECT * from hudi_users_view)")
