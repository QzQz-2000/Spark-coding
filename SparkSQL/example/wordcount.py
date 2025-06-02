from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F

if __name__ == "__main__":
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # 通过SparkSession对象获得SparkContext对象
    sc = spark.sparkContext

    # SQL风格处理
    rdd = sc.textFile("../../RDD/data/input/words.txt").\
        flatMap(lambda x: x.split(" ")).\
        map(lambda x: [x])

    df = rdd.toDF(["word"])
    df.createTempView("words")

    spark.sql("SELECT word, COUNT(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC").show()

    # DSL风格处理
    # text格式没有给schema时，默认列名为value
    df2 = spark.read.format("text").load("../../RDD/data/input/words.txt")

    # withColumn: 对已存在的列进行操作，返回一个新的列，如果名字和已存在的列相同，则进行替换，否则作为新列保存
    df2 = df2.withColumn("value", F.explode(F.split(df2['value'], " ")))
    df2.groupBy("value").\
        count().\
        withColumnRenamed("value", "word").\
        withColumnRenamed("count", "cnt").\
        orderBy("cnt", ascending=False).\
        show()
