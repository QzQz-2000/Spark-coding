from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 2, 3, 4])

    print(rdd1.first())
    # 1 

    print(rdd1.take(3))
    # [1, 2, 3]

    print(rdd1.top(3))
    # [4, 3, 2]

    print(rdd1.count())
    # 4
    