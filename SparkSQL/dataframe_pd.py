from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == "__main__":
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    sc = spark.sparkContext

    # 基于Pandas的df构建sparksql的df
    pdf = pd.DataFrame(
        {
            "id": [1, 2, 4],
            "name": ["aa", "bb", "cc"],
            "age": [11, 12, 13]
        }
    )

    df = spark.createDataFrame(pdf)
    df.printSchema()
    df.show()
