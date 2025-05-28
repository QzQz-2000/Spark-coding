
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1. 读取文件，构建rdd
    file_rdd = sc.textFile("./RDD/data/words.txt")

    # 2. 通过flatMap取出所有的单词
    word_rdd = file_rdd.flatMap(lambda line: line.split(" "))

    # 3. 将单词转换成元组
    word_with_one_rdd = word_rdd.map(lambda word: (word, 1))

    # 4. 用reduceByKey对单词进行分组以及value的聚合
    result_rdd = word_with_one_rdd.reduceByKey(lambda a, b: a+b)

    # 5. 通过collect将rdd的数据收集到driver中打印输出
    print(result_rdd.collect())

    sc.stop()