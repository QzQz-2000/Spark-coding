from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F

if __name__ == "__main__":
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()


    schema = StructType().add("user_id", StringType(), True).\
            add("movie_id", IntegerType(), True).\
            add("rank", IntegerType(), True).\
            add("ts", StringType(), True)

    # 1. 读取数据集
    df = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False).\
        option("encoding", "utf-8").\
        schema(schema).\
        load("../RDD/data/input/u.data")

    # 写出为text数据，只能写出一个列，因此需要将df转换为单列df
    df.select(F.concat_ws("---", "user_id", "movie_id", "rank", "ts")).\
        write.\
        mode("overwrite").\
        format("text").\
        save("../RDD/data/output/sql/text")

    # csv
    df.write.mode("overwrite").\
        format("csv").\
        option("sep", ";").\
        option("header", True).\
        save("../RDD/data/output/sql/csv")

    # json
    df.write.mode("overwrite").\
        format("json").\
        save("../RDD/data/output/sql/json")

    # parquet
    df.write.mode("overwrite").\
        format("parquet").\
        save("../RDD/data/output/sql/parquet")