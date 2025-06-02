from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == "__main__":
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # 通过SparkSession对象获得SparkContext对象
    sc = spark.sparkContext

    rdd = sc.textFile("../RDD/data/input/students.txt").\
        map(lambda x: x.split(',')).\
        map(lambda x: [x[0], int(x[1])])

    # 构建表结构的描述对象
    schema = StructType().add("name", StringType(), False).\
        add("age", IntegerType(), False)
    
    # 基于描述对象创建DataFrame
    df = spark.createDataFrame(rdd, schema = schema)

    df.printSchema()
    df.show()