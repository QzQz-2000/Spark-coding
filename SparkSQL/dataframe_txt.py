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

    # 对于text文件来说，默认一整行就是一列数据，默认列名是value，类型是string

    schema = StructType().add("data", StringType(), False)

    df = spark.read.format("text").\
        schema(schema=schema).\
        load("../RDD/data/input/students.txt")
    
    df.printSchema()
    df.show()