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

    id_col = df['id']
    sub_col = df['subject']

    # dsl
    df.select(["id", "subject"]).show()
    df.select("id", "subject").show()
    df.select(id_col, sub_col).show()

    # filter和where实际上功能是一样的
    df.filter("score < 99").show()
    df.filter(df['score'] < 99).show()

    df.where("score < 99").show()
    df.where(df['score'] < 99).show()

    # df.groupBy的API返回值是groupedData，不是dataframe
    # 它是一个有分组关系的数据结构，调用聚合方法后才是dataframe，所以它只是一个中转对象
    df.groupBy("subject").count().show()
    df.groupBy(df['subject']).count().show()
