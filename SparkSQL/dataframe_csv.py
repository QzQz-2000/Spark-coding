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

    # JSON数据自带schema信息
    df = spark.read.format("csv").\
        option("sep", ";").\
        option("header", True).\
        option("encoding", "utf-8").\
        schema("name STRING, age INT").\
        load("../RDD/data/input/student.csv")

    df.printSchema()
    df.show()