from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == "__main__":
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    df = spark.read.format("csv").\
        schema("id INT, subject STRING, score INT").\
        load("../RDD/data/input/stu_score.txt")

    df.createTempView("score1")
    df.createOrReplaceTempView("score2")
    # 需要加上global_temp的前缀才能查询
    df.createGlobalTempView("score3")

    spark.sql("SELECT subject, COUNT(*) AS cnt FROM score1 GROUP BY subject").show()
    spark.sql("SELECT subject, COUNT(*) AS cnt FROM score2 GROUP BY subject").show()
    spark.sql("SELECT subject, COUNT(*) AS cnt FROM global_temp.score3 GROUP BY subject").show()