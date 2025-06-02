from pyspark.sql import SparkSession

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

    # 构建DataFrame，首先传入rdd，再传入schema
    df = spark.createDataFrame(rdd, schema = ['name', 'age'])

    # 打印DF的表结构
    df.printSchema()

    # 打印数据
    # arg1: 展示多少数据，默认为20
    # arg2: 是否对列进行截断，如果列的数据长度超过20字符，后续内容以...代替，默认是True
    df.show(1, False)

    # 将DF对象转化为临时视图表，可用sql语句查询
    df.createOrReplaceTempView("student")
    spark.sql("SELECT * FROM student WHERE age < 26").show()
    