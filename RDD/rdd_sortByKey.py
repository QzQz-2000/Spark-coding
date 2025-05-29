from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('E', 9), ('c', 1), ('F', 6), ('b', 1), ('G', 5)])

    rdd2 = rdd1.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower())

    print(rdd2.collect())
    # [('a', 1), ('b', 1), ('c', 1), ('E', 9), ('F', 6), ('G', 5)]