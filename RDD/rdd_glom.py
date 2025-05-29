from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)

    print(rdd.glom().collect())
    # [[1], [2, 3], [4, 5]]

    # 如果想解除嵌套，可以用flatmap传入一个空的函数
    print(rdd.glom().flatMap(lambda x: x).collect())