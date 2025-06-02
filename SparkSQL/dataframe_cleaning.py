from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == "__main__":
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    df = spark.read.format("csv").\
        option("sep", ";").\
        option("header", True).\
        load("../RDD/data/input/people.csv")

    # 数据去重
    # 无参数使用，则会所有列联合起来去重
    df.dropDuplicates().show()

    # 有参数时，按照指定的列去重
    df.dropDuplicates(["age", "job"]).show()

    # 缺失值处理
    # 无参数使用时，直接去掉所有有缺失值的行
    df.dropna().show()

    # 有参数时，thresh表示最少三列有效，即没有空
    df.dropna(thresh=3).show()

    # subset参数表示对于子集中的列，需要达到第一个条件
    df.dropna(thresh=2, subset=['name', 'age']).show()

    # 缺失值填充
    df.fillna("loss").show()

    # 指定列进行填充
    df.fillna("N/A", subset=['job']).show()

    # 设定字典，设定所有填充规则
    df.fillna({"name": "unkown", "age": 1, "job": "worker"}).show()