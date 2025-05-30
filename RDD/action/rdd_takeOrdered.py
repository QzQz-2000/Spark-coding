from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 3, 4, 1, 9])

    print(rdd.takeOrdered(3))
    # [1, 1, 2]

    print(rdd.takeOrdered(3, lambda x: -x))
    # [9, 5, 4]